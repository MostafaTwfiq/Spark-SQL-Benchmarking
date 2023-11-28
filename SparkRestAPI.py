import requests
from jinja2 import Template

SPARK_REST_API = 'http://{{ ip }}:{{ port }}/api/v1'
APPICATIONS_API = '/applications/{{ appid }}'
JOBS_API = '/jobs/{{ jobid }}'
SQL_API = '/sql/{{ executionid }}'

class SparkRestAPI:
    def __init__(self, spark_ip, spark_port):
        self.ip = spark_ip
        self.port = spark_port

    def __send_get_request(self, url):
        # Sending a GET request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            return response.json()
        else:
            # If the request was not successful, print the error code
            print(f"Failed to retrieve data. Status code: {response.status_code}")
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
    
    def get_application_sql_metrics(self, application_id, sql_execution_id):
        template_str = f"{SPARK_REST_API}{APPICATIONS_API}{SQL_API}"
        template = Template(template_str)

        placeholders = {"ip": self.ip, "port": self.port, "appid": application_id, "executionid": sql_execution_id}

        url = template.render(placeholders)
        json_data = self.__send_get_request(url)

        return json_data

if __name__ == '__main__':
    rest = SparkRestAPI('localhost', '4040')
    print(rest.get_application_metrics('local-1700750109987')['attempts'][0]['duration'])