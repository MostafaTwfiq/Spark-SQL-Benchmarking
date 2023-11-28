import subprocess

class SparkSubmitExecutor():
    def __init__(self, yarn_ip, yarn_port, config_file_path):
        self.yarn_ip = yarn_ip
        self.yarn_port = yarn_port
        self.config_file_path = config_file_path
        self.spark_submit_command = f"""
            spark-submit \
            --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=2G \
            --conf spark.dynamicAllocation.maxExecutors=1 \
            --conf spark.eventLog.enabled=true \
            --conf "spark.metrics.conf={config_file_path}" \
            --conf spark.yarn.resourcemanager.address={yarn_ip}:{yarn_port}"""
        
    def submit_pyspark(self, file_path, args):
        strings_to_concate = [self.spark_submit_command, file_path] + args
        command = " ".join(strings_to_concate)

        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # TODO: Log Here
        if result.returncode == 0:
            print(f"{file_path} Succeeded.")
            application_id = result.stdout # extract application id from stdout
            return application_id
        else:
            print(f"{file_path} Failed. Output:\n{result.stdout}")
            return None
