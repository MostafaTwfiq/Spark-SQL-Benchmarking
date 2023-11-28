import sys
import uuid
import os
from datetime import datetime
import ConfigurationLoader
import DataGeneration
from TemplateManipulator import HiveManipulator, IcebergManipulator
import sparkSubmitExecutor

root_path = None
logs_path = None
tmp_path = None

def create_curr_run_folders():
    root_path = datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs(root_path)

    logs_path = os.path.join(root_path, 'logs')
    tmp_path = os.path.join(root_path, 'tmp')

    os.makedirs(logs_path)
    os.makedirs(tmp_path)


if __name__ == '__main__':
    #Create new folder for the current app run
    create_curr_run_folders()

    #Load configuration file
    config_file_path = sys.argv[1]
    config_loader = ConfigurationLoader(config_file_path)

    #Generate tpch data
    tpch_gen_path = config_loader.get_tpch_generation_path()
    tpch_scale_factor = config_loader.get_tpch_db_scale_factor()
    data_generator = DataGeneration(tpch_scale_factor)
    data_generator.generate_data(tpch_gen_path)

    hive_temp_manipulator = HiveManipulator()
    iceberg_temp_manipulator = IcebergManipulator()
    for i in range(config_loader.get_groups_size()):
        hive_props = config_loader.get_table_properties(i, 'hive')
        iceberg_props = config_loader.get_table_properties(i, 'iceberg')
        #create dummy database (hive and iceberg)
        database_name = f'benchmarking_{str(uuid.uuid4())}'
        hive_db_temp_path = hive_temp_manipulator.create_database_template(tmp_path)
        iceberg_db_temp_path = iceberg_temp_manipulator.create_database_template(tmp_path)
        #insert data into tables (hive and iceberg)
        #loop and run queries
        #collect metrics
        #plot metrics