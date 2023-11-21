import yaml

class ConfigurationLoader:
    def __init__(self, file_name):
        self.file_name = file_name

    def parse_config_file(self):
        # TODO: Log Here
        print(f'Parsing and Loading Configuration File.')
        with open(self.file_name, 'r') as file:
            data = yaml.safe_load(file)

        if 'groups' in data:
            self.properties_groups = data['groups']
            # for group in data['groups']:
            #     for format in group['table_formats']:
            #         print(format)
        else:
            print("No 'groups' key found.")

    def get_table_properties(self, group_id, table_format):
        return self.properties_groups[group_id]['table_formats'][table_format]
    
    def get_groups_size(self):
        return len(self.properties_groups)
        
    def set_hive_table_props(self, group_id):
        props = self.get_table_properties(group_id, 'hive')

            

if __name__ == "__main__":

    conf_loader = ConfigurationLoader(file_name='config.yaml')
    conf_loader.parse_config_file()
    print(conf_loader.get_groups_size())
