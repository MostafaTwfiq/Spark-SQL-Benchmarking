import subprocess
import re

class SparkSubmitExecutor():
    def __init__(self, yarn_ip, yarn_port, config_file_path):
        self.TRACKING_URL_REGEX = r'tracking URL: https://.*?/(application_\d+_\d+)/'
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

      #  result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, capture_output=True)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        # TODO: Log Here
       # if result.returncode == 0:
        #    print(f"{file_path} Succeeded.")
         #   print("Keys:", result.keys())            
          #  pattern = result.compile(self.TRACKING_URL_REGEX)
           # match = pattern.search(result.stdout)
            #application_id = match.group(1)
            #return application_id
        #else:
         #   print(f"{file_path} Failed. Output:\n{result.stdout}")
          #  return None
