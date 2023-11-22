import json 
class TableLoader:
    def __init__(self, table_properties):
        self.table_properties = table_properties
        
    def set_properties(self):
        pass

    def replace_word_in_file(self, file_path, old_word, new_word):
        # Read the content of the file
        with open(file_path, 'r') as file:
            content = file.read()

        # Make the replacements in memory
        modified_content = content.replace(old_word, new_word)

        # Write the modified content back to the file
        with open(file_path, 'w') as file:
            file.write(modified_content)
        

class HiveLoader(TableLoader):

    def set_properties(self):
        
        self.replace_word_in_file('DataCreationHive.py', '#partitioning_dict', 
                                  json.dumps(self.table_properties['partition']))
        


class IcebergLoader(TableLoader):

    def set_properties(self):
        
        self.replace_word_in_file('DataCreationIceberg.py', '#partitioning_dict', 
                                  self.table_properties['partition'])
        
        self.replace_word_in_file('DataCreationIceberg.py', '#delete_mode', 
                                  self.table_properties['delete_mode'])

        self.replace_word_in_file('DataCreationIceberg.py', '#update_mode', 
                                  self.table_properties['delete_mode'])

        self.replace_word_in_file('DataCreationIceberg.py', '#merge_mode', 
                                  self.table_properties['delete_mode'])
        

from ConfigurationLoader import ConfigurationLoader;

if __name__ == "__main__":

    conf_loader = ConfigurationLoader(file_name='config.yaml')
    
    len = conf_loader.get_groups_size()
    for i in range(len):
        hive_props = conf_loader.get_table_properties(i, 'hive')
        hive_loader = HiveLoader(hive_props)

        hive_loader.set_properties()
    