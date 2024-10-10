import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
#pip install apache-airflow-providers-amazon
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:  
    hook = S3Hook("s3_conn")
    hook.load_file(filename = filename, key = key, bucket_name = bucket_name)  

with DAG(
    dag_id = "s3_upload", 
    schedule_interval = "@daily", 
    start_date = datetime(2024, 10, 8), 
    catchup = False
) as dag: 
    task_upload_to_s3 = PythonOperator(
        task_id = "upload_to_s3", 
        python_callable = upload_to_s3, 
        op_kwargs = {
                'filename': 'demofolder/s3_downloaded_demo1.txt',
                'key': 'requirements.txt',
                'bucket_name': 'akankhya-sample-703403115'
        }
    ) 
