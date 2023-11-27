import sys
import uuid
import ConfigurationLoader
import DataGeneration
import TemplateManipulator

if __name__ == '__main__':
    #Load configuration file
    config_file_path = sys.argv[1]
    config_loader = ConfigurationLoader(config_file_path)

    #Generate tpch data
    tpch_gen_path = config_loader.get_tpch_generation_path()
    tpch_scale_factor = config_loader.get_tpch_db_scale_factor()
    data_generator = DataGeneration(tpch_scale_factor)
    data_generator.generate_data(tpch_gen_path)

    #Create dummy database
    database_name = f'benchmarking_{str(uuid.uuid4())}'
    hive_temp_manipulator = TemplateManipulator()


    #Create Hive and Iceberg tables
    #Insert generated data into tables
    #Run queries and collect metrics
    #Log and plot mertrics