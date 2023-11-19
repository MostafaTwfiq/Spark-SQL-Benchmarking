import json
import requests
from statistics import median

SPARK_URL = 'http://localhost:4040/api/v1'

def create_tables(group_id, partition, sort, insert_mode, delete_mode, update_mode):
    print(f"Creating tables for Group {group_id} with properties:")
    print(f"Partition: {partition}")
    print(f"Sort: {sort}")
    print(f"InsertMode: {insert_mode}")
    print(f"DeleteMode: {delete_mode}")
    print(f"UpdateMode: {update_mode}")
    
def apply_query():
    pass

def drop_tables():
    pass

def get_current_process_id():

    url = f'{SPARK_URL}/applications?status=running&limit=1'
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.text)
        id = data[0]['id']
        print(f"Process ID = {id}")
        return id
    else:
        print(f"Error Getting Process: {response.status_code}")

def request_metrics():
    id = get_current_process_id()
    url = f'{SPARK_URL}/applications/{id}/sql'
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.text)

        # Extract durations
        durations = [entry['duration'] for entry in data]

        # Calculate the median
        median_duration = median(durations)

        print(f"The median duration is: {median_duration}")
    else:
        print(f"Error: {response.status_code}")
    


def main():

    with open('config.json', 'r') as file:
        config_data = json.load(file)
    
    TRIALS_NUM = config_data['trials_num']

    for group in config_data['query_groups']:
        
        group_id = group['group_id']
        
        create_tables(group_id, group['partition'], group['sort'],
                       group['insert_mode'], group['delete_mode'],
                       group['update_mode'])
        
        for _ in range(TRIALS_NUM):
            apply_query()

        request_metrics()

        drop_tables()

if __name__ == "__main__":
    main()
