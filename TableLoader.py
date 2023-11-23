import os
import json

class TableManipulator:
        
    def set_properties(self):
        pass

    def set_query(self):
        pass

    def replace_words_in_file(self, file_path, modifications):
        # Read the content of the file
        with open(file_path, 'r') as file:
            content = file.read()

        for (old_word, new_word) in modifications:
            # Make the replacements in memory
            content = content.replace(old_word, new_word)

        base_name, extension = os.path.splitext(file_path)
        new_file_path = f'{base_name}_temp{extension}'

        # Write the modified content back to the file
        with open(new_file_path, 'w') as file:
            file.write(content)
        

class HiveManipulator(TableManipulator):

    def set_properties(self, table_properties):
        self.replace_words_in_file('DataCreationHive.py', [("'[partitioning_dict]'", 
                                  json.dumps(table_properties['partition']))])
        
    def set_query(self, query):
        self.replace_words_in_file('QueryHive.py', [("[QUERY]", query)])



class IcebergManipulator(TableManipulator):

    def set_properties(self, table_properties):
        modifications = [("'[partitioning_dict]'", 
                              json.dumps(table_properties['partition'])), 
                              ('[DELETE_MODE]', table_properties['delete_mode']),
                              ('[UPDATE_MODE]', table_properties['update_mode']),
                              ('[MERGE_MODE]', table_properties['merge_mode'])]
        
        self.replace_words_in_file('DataCreationIceberg.py', modifications)
        
        
    def set_query(self, query):
        self.replace_words_in_file('QueryIceberg.py', [("[QUERY]", query)])
        

# from ConfigurationLoader import ConfigurationLoader;

# if __name__ == "__main__":

#     conf_loader = ConfigurationLoader(file_name='config.yaml')
    
#     len = conf_loader.get_groups_size()
#     for i in range(len):
#         hive_props = conf_loader.get_table_properties(i, 'hive')
#         hive_loader = HiveManipulator()
#         hive_loader.set_properties(hive_props)
#         hive_loader.set_query("select * from nation;")

#         iceberg_props = conf_loader.get_table_properties(i, 'iceberg')
#         iceberg_loader = IcebergManipulator()
#         print(iceberg_props)
#         iceberg_loader.set_properties(iceberg_props)
#         iceberg_loader.set_query("select * from nation;")


    