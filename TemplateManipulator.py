import os
import json
from jinja2 import Template
import uuid
import subprocess
from Utils import get_random_string

class TemplateManipulator:

    def __init__(self, output_folder):
        self.TEMPLATES_FOLDER = './Templates'
        self.QUERY_PLACEHOLDER = 'QUERY'
        self.output_folder = output_folder
        self.__add_shemas_to_temp_folder()

    def __add_shemas_to_temp_folder(self):
        command = f'cp {self.TEMPLATES_FOLDER}/schemas.py {self.output_folder}'
        subprocess.run(command, shell=True, stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, text=True)
        
    def set_creation_template_properties(self, table_properties, generated_tables_folder):
        pass

    def set_query(self, query):
        pass

    def replace_words_in_file(self, input_file, output_file, modifications):
        # Read the content of the file
        with open(input_file, 'r') as file:
            template_content = file.read()

        template = Template(template_content)
        modified_content = template.render(modifications)

        # Write the modified content back to the file
        with open(output_file, 'w') as file:
            file.write(modified_content)
        

class HiveManipulator(TemplateManipulator):

    def __init__(self, output_folder):
        super().__init__(output_folder)
        self.PARTITION_PLACEHOLDER = 'PARTITIONING_DICT'
        self.GENERATE_TABLES_FOLDER = 'GENERATED_TABLES_FOLDER'


    def set_creation_template_properties(self, table_properties, generated_tables_folder):
        partitioning_dict = self.__extract_or_default_properties(table_properties)
        print("HELLOOOO " + generated_tables_folder)
        
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'DataCreationHive.py')
        output_file_path = os.path.join(self.output_folder, f'DataCreationHive_{get_random_string(3)}.py')
        try:
            self.replace_words_in_file(input_file_path, output_file_path, {self.PARTITION_PLACEHOLDER: partitioning_dict,
                                    self.GENERATE_TABLES_FOLDER: generated_tables_folder})
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None
        

    def __extract_or_default_properties(self, table_properties):
        partitioning_dict = '{}'

        if table_properties['partition'] is not None:
            partitioning_dict = json.dumps(table_properties['partition'])
        
        return partitioning_dict
    
    def set_query(self, query):
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'QueryHive.py')
        output_file_path = os.path.join(self.output_folder, f'QueryHive_{get_random_string(3)}.py')

        try:
            self.replace_words_in_file(input_file_path, output_file_path, {self.QUERY_PLACEHOLDER: query})
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None
        
    def create_database_template(self):
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'HiveDataBaseCreation.py')
        print(self.output_folder)
        output_file_path = os.path.join(self.output_folder, f'HiveDataBaseCreation_{get_random_string(3)}.py')

        try:
            self.replace_words_in_file(input_file_path, output_file_path, {})
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None




class IcebergManipulator(TemplateManipulator):

    def __init__(self, output_folder):
        super().__init__(output_folder)
        self.PARTITION_PLACEHOLDER = 'PARTITIONING_DICT'
        self.DELETE_PLACEHOLDER = 'DELETE_MODE'
        self.UPDATE_PLACEHOLDER = 'UPDATE_MODE'
        self.MERGE_PLACEHOLDER = 'MERGE_MODE'
        self.GENERATE_TABLES_FOLDER = 'GENERATED_TABLES_FOLDER'


    def set_creation_template_properties(self, table_properties, generated_tables_folder):
        partitioning_dict, delete_mode, update_mode, merge_mode \
            = self.__extract_or_default_properties(table_properties) 
        
        modifications = {self.PARTITION_PLACEHOLDER: partitioning_dict, 
                         self.DELETE_PLACEHOLDER: delete_mode,
                         self.UPDATE_PLACEHOLDER: update_mode,
                         self.MERGE_PLACEHOLDER: merge_mode,
                         self.GENERATE_TABLES_FOLDER: generated_tables_folder}
        
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'DataCreationIceberg.py')
        output_file_path = os.path.join(self.output_folder, f'DataCreationIceberg_{get_random_string(3)}.py')
        try:
            self.replace_words_in_file(input_file_path, output_file_path, modifications)
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None


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

    def set_query(self, query):
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'QueryIceberg.py')
        output_file_path = os.path.join(self.output_folder, f'QueryIceberg_{get_random_string(3)}.py')

        try:
            self.replace_words_in_file(input_file_path, output_file_path, {self.QUERY_PLACEHOLDER: query})
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None  

    def create_database_template(self):
        input_file_path = os.path.join(self.TEMPLATES_FOLDER, 'IcebergDataBaseCreation.py')
        output_file_path = os.path.join(self.output_folder, f'IcebergDataBaseCreation_{get_random_string(3)}.py')

        try:
            self.replace_words_in_file(input_file_path, output_file_path, {})
            return output_file_path
        except Exception as e:
            # TODO: Log the exception here
            print(f"An error occurred: {e}")
            return None    
    

from ConfigurationLoader import ConfigurationLoader;

if __name__ == "__main__":
    print(get_random_string(3))

    # conf_loader = ConfigurationLoader(conf_path='./config.yaml')
    
    # len = conf_loader.get_groups_size()
    # for i in range(len):
    #     hive_props = conf_loader.get_table_properties(i, 'hive')
    #     hive_loader = HiveManipulator('./temp')
    #     hive_loader.set_creation_template_properties(hive_props, 'HI')
    #     hive_loader.set_query("select * from nation;")

    #     iceberg_props = conf_loader.get_table_properties(i, 'iceberg')
    #     iceberg_loader = IcebergManipulator('./temp')
    #     iceberg_loader.set_creation_template_properties(iceberg_props, 'HI')
    #     iceberg_loader.set_query("select * from nation;")
