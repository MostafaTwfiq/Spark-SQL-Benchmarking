
SPARK_REST_API = 'http://[ip]:[port]/api/v1'
APPICATIONS_API = '/applications/[app-id]'
JOBS_API = '/jobs/[job-id]'
SQL_API = '/sql/[execution-id]'

class SpaekRestAPI:
    def __init__(self, spark_ip, spark_port):
        self.ip = spark_ip
        self.port = spark_port

    #def get_application_metrics(self, application_id):