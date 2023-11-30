import sys
import uuid
import os
from datetime import datetime
from ConfigurationLoader import ConfigurationLoader
from DataGeneration import DataGeneration
from TemplateManipulator import HiveManipulator, IcebergManipulator
from SparkRestAPI import SparkRestAPI
from MetricsPlotter import MetricsPlotter
from SparkSubmitExecutor import SparkSubmitExecutor
import re 
import subprocess
import shutil

root_path = None
logs_path = None
tmp_path = None
metric_path = None
spark_config_file = './resources/metrics.properties'

def create_curr_run_folders():
    global root_path, logs_path, tmp_path, metric_path
    root_path = datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs(root_path)

    logs_path = os.path.join(root_path, 'logs')
    tmp_path = os.path.join(root_path, 'tmp')
    metric_path = os.path.join(root_path, 'metrics_plots')

    os.makedirs(logs_path)
    os.makedirs(tmp_path)
    os.makedirs(metric_path)

def read_benchmark_queries(sql_file_path):
    with open(sql_file_path, 'r') as file:
        data = file.read()

    # Use regular expressions to extract both comments and queries
    queries_names = re.findall(r'--(.*?)(?=\n|$)', data)
    queries = re.split(r'--.*?(?=\n|$)|;\s*', data)

    # Remove leading and trailing whitespace from each query
    queries = [query.strip() for query in queries]

    # Remove empty queries
    queries = [query for query in queries if query]

    # Remove unnecessary whitespace from each query
    queries = [' '.join(query.split()) + ";" for query in queries]

    return queries_names, queries

def copy_banchmark_data_to_hdfs(hdfs_ip, hdfs_port, hdfs_user_path, tpch_gen_path):
    benchmarking_tmp_path = f'benchmarking_tmp_tpch_data_{str(uuid.uuid4()).replace("-", "_")}'

    create_hdfs_folder_command = f'hadoop fs -mkdir hdfs://{hdfs_ip}:{hdfs_port}/{hdfs_user_path}/{benchmarking_tmp_path}'
    result = subprocess.run(create_hdfs_folder_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"hdfs folder {benchmarking_tmp_path} creation Succeeded.") if result.returncode == 0 else print(f"hdfs folder {benchmarking_tmp_path} creation Failed. Output:\n{result.stdout}")    

    copy_tpch_data_to_hdfs_command = f'hdfs dfs -copyFromLocal {tpch_gen_path}/* hdfs://{hdfs_ip}:{hdfs_port}/{hdfs_user_path}/{benchmarking_tmp_path}'
    result = subprocess.run(copy_tpch_data_to_hdfs_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"Copying tpch data to {benchmarking_tmp_path} Succeeded.") if result.returncode == 0 else print(f"Copying tpch data to {benchmarking_tmp_path} Failed. Output:\n{result.stdout}")     

    return benchmarking_tmp_path

def delete_hdfs_folder(hdfs_ip, hdfs_port, hdfs_user_path, folder_path):
    remove_hdfs_folder_command = f'hdfs dfs -rm -r hdfs://{hdfs_ip}:{hdfs_port}/{hdfs_user_path}/{folder_path}'
    result = subprocess.run(remove_hdfs_folder_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"{folder_path} hdfs folder deletion Succeeded.") if result.returncode == 0 else print(f"{folder_path} hdfs folder deletion Failed. Output:\n{result.stdout}")    


if __name__ == '__main__':
    # Create new folder for the current app run
    create_curr_run_folders()
    print("Benchmark folders created successfully.") # Logging

    # Load configuration file
    config_file_path = sys.argv[1]
    config_loader = ConfigurationLoader(config_file_path)
    print("Configuration file loaded successfully.") # Logging

    # Generate tpch data
    tpch_dbgen_path = './tpch-dbgen'#sys.argv[2]
    tpch_gen_path = config_loader.get_tpch_generation_path()
    tpch_scale_factor = config_loader.get_tpch_db_scale_factor()
    data_generator = DataGeneration(tpch_dbgen_path, tpch_scale_factor)
    data_generator.generate_data(tpch_gen_path)
    print("Generated benchmark data successfully.")

    # Copy generated data to hdfs
    hdfs_conn = config_loader.get_hdfs_connection()
    hdfs_tpch_data_path = copy_banchmark_data_to_hdfs(hdfs_conn['ip'], hdfs_conn['port'], hdfs_conn['user_folder_path'], tpch_gen_path)
    print("Benchmark data copied to hdfs successfully.")

    # Benchmarking
    spark_connection = config_loader.get_spark_connection()
    hdfs_connection = config_loader.get_hdfs_connection()
    hive_connection = config_loader.get_hive_connection()
    iceberg_warehouse = config_loader.get_iceberg_warehouse()
    yarn_connection = config_loader.get_yarn_connection()
    spark_submit_executor = SparkSubmitExecutor(yarn_connection['ip'], yarn_connection['port'], spark_config_file)

    hive_temp_manipulator = HiveManipulator(tmp_path)
    iceberg_temp_manipulator = IcebergManipulator(tmp_path)

    for i in range(config_loader.get_groups_size()):
        print(f"Benchmarking properties group {i}:") #Logging
        
        # Loading current properties group
        hive_props = config_loader.get_table_properties(i, 'hive')
        iceberg_props = config_loader.get_table_properties(i, 'iceberg')

        # Create dummy database (hive and iceberg)
        hive_database_name = f'benchmarking_{str(uuid.uuid4()).replace("-", "_")}'   
        iceberg_database_name = f'benchmarking_{str(uuid.uuid4()).replace("-", "_")}'
        
        hive_connection_args = [hive_connection['ip'], str(hive_connection['port']), hive_database_name]
        iceberg_connection_args = [hdfs_connection['ip'], str(hdfs_connection['port']), iceberg_database_name, iceberg_warehouse]

        hive_db_temp_path = hive_temp_manipulator.create_database_template()
        print(f"\tDone creating hive database template: {hive_db_temp_path}") # Logging
        iceberg_db_temp_path = iceberg_temp_manipulator.create_database_template()
        print(f"\tDone creating iceberg database template: {iceberg_db_temp_path}") # Logging
        spark_submit_executor.submit_pyspark(hive_db_temp_path, hive_connection_args)
        print(f"\tDone creating hive database: {hive_database_name}") # Logging
        spark_submit_executor.submit_pyspark(iceberg_db_temp_path, iceberg_connection_args)
        print(f"\tDone creating iceberg database: {iceberg_database_name}") # Logging

        #insert data into tables (hive and iceberg)
        hive_insertion_temp_path = hive_temp_manipulator. \
            set_creation_template_properties(config_loader.get_table_properties(i, 'hive'), hdfs_tpch_data_path)
        print(f"\tDone creating hive records insertion template: {hive_insertion_temp_path}") # Logging
        iceberg_insertion_temp_path = iceberg_temp_manipulator. \
            set_creation_template_properties(config_loader.get_table_properties(i, 'iceberg'), hdfs_tpch_data_path)
        print(f"\tDone creating iceberg records insertion template: {iceberg_insertion_temp_path}") # Logging
        spark_submit_executor.submit_pyspark(hive_insertion_temp_path, hive_connection_args)
        print(f"\tRecords successfully inserted in hive tables.") # Logging
        spark_submit_executor.submit_pyspark(iceberg_insertion_temp_path, iceberg_connection_args)
        print(f"\tRecords successfully inserted in iceberg tables.") # Logging
        
        # Loop and run queries
        hive_durations = []
        iceberg_durations = []
        rest = SparkRestAPI(spark_connection['ip'], spark_connection['port'])
        queries_names, dql_benchmark_queries = read_benchmark_queries('benchmark_queries.sql')
        print("\tBenchmark queries loaded successfully.")
        for query_name, query in zip(queries_names, dql_benchmark_queries):
            print(f"\t Benchmarking {query_name} query:")

            # Creating Output Scripts
            hive_query_temp_path = hive_temp_manipulator.set_query(query)
            print(f"\t\tDone creating hive SQL execution template: {hive_query_temp_path}") # Logging
            iceberg_query_temp_path = iceberg_temp_manipulator.set_query(query)
            print(f"\t\tDone creating iceberg SQL execution template: {iceberg_query_temp_path}") # Logging
            
            # Submit Output Scripts to spark-submit
            hive_app_id = spark_submit_executor.submit_pyspark(hive_query_temp_path, hive_connection_args)
            print(f"\t\tCompleted running the query on hive.") # Logging
            iceberg_app_id = spark_submit_executor.submit_pyspark(iceberg_query_temp_path, iceberg_connection_args)
            print(f"\t\tCompleted running the query on iceberg.") # Logging
        
            # Fetch Query Duration
            hive_durations.append(rest.get_sql_duration(rest.get_application_all_sql_metrics(hive_app_id)))
            print("\t\tQuery duration on hive fetched successfully.") # Logging
            iceberg_durations.append(rest.get_sql_duration(rest.get_application_all_sql_metrics(iceberg_app_id)))
            print("\t\tQuery duration on iceberg fetched successfully.") # Logging
                    
        # Plot metrics
        plotter = MetricsPlotter(metric_path)
        plotter.plot_benchmark_results(queries=queries_names, 
                                   tables_metrics={'hive': hive_durations, 'iceberg': iceberg_durations},
                                   metric_type='duration', title='Hive vs Iceberg')
        print("\tDone plotting metrics.")
    

    hive_db_deletion_temp_path = hive_temp_manipulator.create_database_deletion_template()
    print(f"\tDone creating hive database deletion template: {hive_db_deletion_temp_path}") # Logging
    iceberg_db_deletion_temp_path = iceberg_temp_manipulator.create_database_deletion_template()
    print(f"\tDone creating iceberg database deletion template: {iceberg_db_deletion_temp_path}") # Logging
    spark_submit_executor.submit_pyspark(hive_db_deletion_temp_path, hive_connection_args)
    print(f"\tDone deleting hive database: {hive_db_deletion_temp_path}") # Logging
    spark_submit_executor.submit_pyspark(iceberg_db_deletion_temp_path, iceberg_connection_args)
    print(f"\tDone deleting iceberg database: {iceberg_db_deletion_temp_path}") # Logging
    
    try:
        shutil.rmtree(tmp_path)
        print(f"{tmp_path} deleted successfully.") # Logging
    except OSError as e:
        print(f"Error occurred while deleting {tmp_path}: {e}")
    print(f"{tmp_path} deleted successfully.") # Logging
    delete_hdfs_folder(hdfs_conn['ip'], hdfs_conn['port'], hdfs_conn['user_folder_path'], hdfs_tpch_data_path)
    print(f'{hdfs_tpch_data_path} hdfs folder deleted successfully.') # Logging