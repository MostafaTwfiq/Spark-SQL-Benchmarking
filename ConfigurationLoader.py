import yaml

class ConfigurationLoader:
    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.__parse_config_file()

    def __parse_config_file(self):
        # TODO: Log Here
        print(f'Parsing and Loading Configuration File.')
        with open(self.conf_path, 'r') as file:
            data = yaml.safe_load(file)

        self.connections = data['connections']
        if 'groups' in data:
            self.properties_groups = data['groups']
            # for group in data['groups']:
            #     for format in group['table_formats']:
            #         print(format)
        else:
            print("No 'groups' key found.")

    def get_table_properties(self, group_id, table_format):
        return self.properties_groups[group_id]['table_formats'][table_format]
    
    def get_properties_group_db_scale_factor(self, group_id):
        return self.properties_groups[group_id]['database_scale_factor']
    
    def get_groups_size(self):
        return len(self.properties_groups)
    
    def get_spark_connection(self):
        return self.connections['spark']
    
    def get_hive_connection(self):
        return self.connections['hive']
    
    def get_iceberg_connection(self):
        return self.connections['iceberg']
    
            

if __name__ == "__main__":
    conf_loader = ConfigurationLoader(conf_path='config.yaml')
    print(conf_loader.get_spark_connection())
    print(conf_loader.get_hive_connection())
    print(conf_loader.get_iceberg_connection())
    len = conf_loader.get_groups_size()
    print(conf_loader.get_properties_group_db_scale_factor(0))
    for i in range(len):
        hive_props = conf_loader.get_table_properties(i, 'hive')
        iceberg_props = conf_loader.get_table_properties(i, 'iceberg')
