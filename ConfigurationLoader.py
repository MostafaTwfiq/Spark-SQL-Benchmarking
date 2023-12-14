import yaml

class ConfigurationLoader:
    def __init__(self, conf_path, logger):
        self.conf_path = conf_path
        self.logger = logger
        self.__parse_config_file()

    def __parse_config_file(self):
        # TODO: Log Here
        self.logger.info(f'Parsing and Loading Configuration File.')
        with open(self.conf_path, 'r') as file:
            data = yaml.safe_load(file)

        self.connections = data['connections']
        self.tpch = data['tpch']
        self.properties_groups = data['groups']

    def get_table_properties(self, group_id, table_format):
        return self.properties_groups[group_id]['table_formats'][table_format]
    
    def get_tpch_generation_path(self):
        return self.tpch['generation_path']
    
    def get_tpch_db_scale_factor(self):
        return self.tpch['database_scale_factor']
    
    def get_groups_size(self):
        return len(self.properties_groups)
    
    def get_spark_connection(self):
        return self.connections['spark']
    
    def get_yarn_connection(self):
        return self.connections['yarn']
    
    def get_hive_connection(self):
        return self.connections['hive']
    
    def get_hdfs_connection(self):
        return self.connections['hdfs']
    
    def get_iceberg_warehouse(self):
        return self.connections['iceberg']['iceberg_warehouse']
    
            

if __name__ == "__main__":
    conf_loader = ConfigurationLoader(conf_path='config.yaml')
    self.logger.info(conf_loader.get_spark_connection())
    self.logger.info(conf_loader.get_hive_connection())
    len = conf_loader.get_groups_size()
    self.logger.info(conf_loader.get_tpch_generation_path())
    self.logger.info(conf_loader.get_tpch_db_scale_factor())
    self.logger.info(conf_loader.get_hdfs_connection())
    self.logger.info(conf_loader.get_iceberg_warehouse())
    for i in range(len):
        hive_props = conf_loader.get_table_properties(i, 'hive')
        self.logger.info(hive_props)
        iceberg_props = conf_loader.get_table_properties(i, 'iceberg')
        self.logger.info(iceberg_props)
