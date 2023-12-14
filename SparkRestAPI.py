import requests
from jinja2 import Template
import json


SPARK_REST_API = 'https://{{ ip }}:{{ port }}/api/v1'
APPICATIONS_API = '/applications/{{ appid }}'
JOBS_API = '/jobs/{{ jobid }}'
ALL_SQL_API = '/sql/'
SQL_API = f'{ALL_SQL_API}{{ executionid }}'

class SparkRestAPI:
    def __init__(self, spark_ip, spark_port, logger):
        self.ip = spark_ip
        self.port = spark_port
        self.logger = logger

    def __send_get_request(self, url):
        # Sending a GET request
        response = requests.get(url, verify=False)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            return response.json()
        else:
            # If the request was not successful, self.logger.info the error code
            self.logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
            #TODO: log an error with the url and response code


    def get_application_metrics(self, application_id):
        template_str = f"{SPARK_REST_API}{APPICATIONS_API}"
        template = Template(template_str)

        placeholders = {"ip": self.ip, "port": self.port, "appid": application_id}

        url = template.render(placeholders)
        json_data = self.__send_get_request(url)

        return json_data
    
    def get_application_job_metrics(self, application_id, job_id):
        template_str = f"{SPARK_REST_API}{APPICATIONS_API}{JOBS_API}"
        template = Template(template_str)

        placeholders = {"ip": self.ip, "port": self.port, "appid": application_id, "jobid": job_id}

        url = template.render(placeholders)
        json_data = self.__send_get_request(url)

        return json_data
    
    def get_application_all_sql_metrics(self, application_id):
        template_str = f"{SPARK_REST_API}{APPICATIONS_API}{ALL_SQL_API}"
        template = Template(template_str)

        placeholders = {"ip": self.ip, "port": self.port, "appid": application_id}

        url = template.render(placeholders)
        json_data = self.__send_get_request(url)

        return json_data
    
    def get_application_sql_metrics(self, application_id, sql_execution_id):
        template_str = f"{SPARK_REST_API}{APPICATIONS_API}{ALL_SQL_API}{SQL_API}"
        template = Template(template_str)

        placeholders = {"ip": self.ip, "port": self.port, "appid": application_id, "executionid": sql_execution_id}

        url = template.render(placeholders)
        json_data = self.__send_get_request(url)

        return json_data
    
    def get_application_duration(self, metrics):
        return metrics["attempts"][-1]["duration"]
    
    def get_sql_duration(self, metrics, sql_id = -1):
        return metrics[sql_id]["duration"]


if __name__ == '__main__':
    rest = SparkRestAPI('192.168.168.90', '18489')
    #self.logger.info(rest.get_application_metrics('application_1700037879106_0121'))
    self.logger.info(rest.get_application_duration(rest.get_application_metrics('application_1700037879106_0121')))
    self.logger.info(rest.get_sql_duration(rest.get_application_all_sql_metrics('application_1700037879106_0121')))