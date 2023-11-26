import os
import json

class TableManipulator:

    def __init__(self):
        self.QUERY_PLACEHOLDER = "[QUERY]"
        
    def set_properties(self):
        pass

    def set_query(self, query):
        self.replace_words_in_file('Query.py', [(self.QUERY_PLACEHOLDER, query)])

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

    def __init__(self):
        super().__init__()
        self.PARTITION_PLACEHOLDER = "'[partitioning_dict]'"


    def set_properties(self, table_properties):
        partitioning_dict = self.__extract_or_default_properties(table_properties) 

        self.replace_words_in_file('DataCreationHive.py', 
                                   [(self.PARTITION_PLACEHOLDER, partitioning_dict)])
        

    def __extract_or_default_properties(self, table_properties):
        partitioning_dict = '{}'

        if table_properties['partition'] is not None:
            partitioning_dict = json.dumps(table_properties['partition'])
        
        return partitioning_dict



class IcebergManipulator(TableManipulator):

    def __init__(self):
        super().__init__()
        self.PARTITION_PLACEHOLDER = "'[partitioning_dict]'"
        self.DELETE_PLACEHOLDER = '[DELETE_MODE]'
        self.UPDATE_PLACEHOLDER = '[UPDATE_MODE]'
        self.MERGE_PLACEHOLDER = '[MERGE_MODE]'


    def set_properties(self, table_properties):
        partitioning_dict, delete_mode, update_mode, merge_mode \
            = self.__extract_or_default_properties(table_properties) 
        
        modifications = [(self.PARTITION_PLACEHOLDER, partitioning_dict), 
                         (self.DELETE_PLACEHOLDER, delete_mode),
                         (self.UPDATE_PLACEHOLDER, update_mode),
                         (self.MERGE_PLACEHOLDER, merge_mode)]
        
        self.replace_words_in_file('DataCreationIceberg.py', modifications)


    def __extract_or_default_properties(self, table_properties):
        delete_mode = update_mode = merge_mode = 'merge-on-read'
        partitioning_dict = '{}'

        if table_properties['partition'] is not None:
            partitioning_dict = json.dumps(table_properties['partition'])

        if 'delete_mode' in table_properties:
            delete_mode = table_properties['delete_mode']

        if 'update_mode' in table_properties:
            update_mode = table_properties['update_mode']            

        if 'merge_mode' in table_properties:
            merge_mode = table_properties['merge_mode']

        return partitioning_dict, delete_mode, update_mode, merge_mode

            

# from ConfigurationLoader import ConfigurationLoader;

# if __name__ == "__main__":

#     conf_loader = ConfigurationLoader(conf_path='./config.yaml')
    
#     len = conf_loader.get_groups_size()
#     for i in range(len):
#         hive_props = conf_loader.get_table_properties(i, 'hive')
#         hive_loader = HiveManipulator()
#         hive_loader.set_properties(hive_props)
#         hive_loader.set_query("select * from nation;")

#         iceberg_props = conf_loader.get_table_properties(i, 'iceberg')
#         iceberg_loader = IcebergManipulator()
#         iceberg_loader.set_properties(iceberg_props)
#         iceberg_loader.set_query("select * from nation;")


    