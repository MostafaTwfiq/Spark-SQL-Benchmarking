import sys
import uuid
import os
from datetime import datetime
import ConfigurationLoader
import DataGeneration
from TemplateManipulator import HiveManipulator, IcebergManipulator
import SparkRestAPI
import MetricsPlotter
import SparkSubmitExecutor
import re 

root_path = None
logs_path = None
tmp_path = None
metric_path = None
spark_config_file = 'resources\metrics.properties'

def create_curr_run_folders():
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
    queries = re.split(r';\s*', data)

    # Remove leading and trailing whitespace from each query
    queries = [query.strip() for query in queries]

    # Remove empty queries
    queries = [query for query in queries if query]

    # Remove unnecessary whitespace from each query
    queries = [' '.join(query.split()) + ";" for query in queries]

    return queries_names, queries

if __name__ == '__main__':
    # Create new folder for the current app run
    create_curr_run_folders()

    # Load configuration file
    config_file_path = sys.argv[1]
    config_loader = ConfigurationLoader(config_file_path)

    # Generate tpch data
    tpch_gen_path = config_loader.get_tpch_generation_path()
    tpch_scale_factor = config_loader.get_tpch_db_scale_factor()
    data_generator = DataGeneration(tpch_scale_factor)
    data_generator.generate_data(tpch_gen_path)

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
        hive_props = config_loader.get_table_properties(i, 'hive')
        iceberg_props = config_loader.get_table_properties(i, 'iceberg')
        # Create dummy database (hive and iceberg)
        database_name = f'benchmarking_{str(uuid.uuid4())}'
        hive_connection_args = [hive_connection['ip'], hive_connection['port'], database_name]
        iceberg_connection_args = [hdfs_connection['ip'], hdfs_connection['port'], database_name, iceberg_warehouse]

        hive_db_temp_path = hive_temp_manipulator.create_database_template()
        iceberg_db_temp_path = iceberg_temp_manipulator.create_database_template()
        spark_submit_executor.submit_pyspark(hive_db_temp_path, hive_connection_args)
        spark_submit_executor.submit_pyspark(iceberg_db_temp_path, iceberg_connection_args)

        #insert data into tables (hive and iceberg)
        hive_insertion_temp_path = hive_temp_manipulator. \
            set_creation_template_properties(config_loader.get_table_properties(i, 'hive'), tpch_gen_path)
        iceberg_insertion_temp_path = iceberg_temp_manipulator. \
            set_creation_template_properties(config_loader.get_table_properties(i, 'iceberg'), tpch_gen_path)
        spark_submit_executor.submit_pyspark(hive_insertion_temp_path, hive_connection_args)
        spark_submit_executor.submit_pyspark(iceberg_insertion_temp_path, iceberg_connection_args)
        
        # Loop and run queries
        hive_durations = []
        iceberg_durations = []
        rest = SparkRestAPI(spark_connection['ip'], spark_connection['port'])
        queries_names, dql_benchmark_queries = read_benchmark_queries('benchmark_queries.sql')

        for query in dql_benchmark_queries:
            # Creating Output Scripts
            hive_query_temp_path = hive_temp_manipulator.set_query(query)
            iceberg_query_temp_path = iceberg_temp_manipulator.set_query(query)
            
            # Submit Output Scripts to spark-submit
            hive_app_id = spark_submit_executor.submit_pyspark(hive_query_temp_path, hive_connection_args)
            iceberg_app_id = spark_submit_executor.submit_pyspark(iceberg_query_temp_path, iceberg_connection_args)
        
            # Fetch Query Duration
            hive_durations.append(rest.get_sql_duration(rest.get_application_all_sql_metrics(hive_app_id)))
            iceberg_durations.append(rest.get_sql_duration(rest.get_application_all_sql_metrics(iceberg_app_id)))
                    
        # Plot metrics
        plotter = MetricsPlotter()
        plotter.plot_benchmark_results(queries=queries_names, 
                                   tables_metrics={'hive': hive_durations, 'iceberg': iceberg_durations},
                                   metric_type='duration', title='Hive vs Iceberg')