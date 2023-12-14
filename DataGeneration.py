import subprocess

class DataGeneration:
    def __init__(self, dbgen_path, logger, scale_factor=5):
        self.dbgen_path = dbgen_path
        self.logger = logger
        self.scale_factor = scale_factor


    def generate_data(self, data_path='./data'):
        # TODO: Log Here
        self.logger.info(f"Generating Data with Scale Factor = {self.scale_factor}")
        self.__precheck_data_directory(data_path)
        
        generate_cmd = f'cd {data_path} && ./dbgen -s {self.scale_factor}'
        self.__execute_command(generate_cmd, 'Data Generation')


    def __precheck_data_directory(self, data_path):
        # Remove Old Data Folder, If exists
        remove_cmd = f'rm -r {data_path}'
        self.__execute_command(remove_cmd, 'Data Directory Deletion')
        
        make_dir_cmd = f'mkdir {data_path}'
        self.__execute_command(make_dir_cmd, 'Data Directory Creation')

        copy_cmd = f'cp {self.dbgen_path}/dbgen {data_path}'
        self.__execute_command(copy_cmd, 'dbgen executable file Copy')

        copy_cmd = f'cp {self.dbgen_path}/dists.dss {data_path}'
        self.__execute_command(copy_cmd, 'dists.dss file Copy')


    def __execute_command(self, command, process):
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, text=True)
        
        # TODO: Log Here
        if result.returncode == 0:
            self.logger.info(f"{process} Succeeded.")
        else:
            self.logger.error(f"{process} Failed. Output:\n{result.stdout}")
                
        


if __name__ == "__main__":

    data_generator = DataGeneration(scale_factor=1)
    data_generator.generate_data(data_path='./data')
