from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

dict_args = {
    "owner": "main",
    "start_date": datetime(2024, 1, 1, 00)
}

def data_stream():
    # get data from api
    res = requests.get("https://randomuser.me/api/")
    
    # to json
    print(res.json())

with DAG(
    "dag_1",
    default_args=dict_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    task_streaming = PythonOperator(
        task_id="stream_data_from_api",
        python_calllable=data_stream
    )    






