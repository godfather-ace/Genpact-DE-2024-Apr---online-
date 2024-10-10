import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
#pip install apache-airflow-providers-amazon
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def download_from_s3(key: str, bucket_name: str, local_path: str): 
    hook = S3Hook("s3_conn")
    file_name = hook.download_file(key = key, bucket_name = bucket_name, local_path = local_path)
    return file_name  

def rename_file(ti, new_name: str) -> None: 
    downloaded_file_name = ti.xcom_pull(task_ids = ['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src = downloaded_file_name[0], dst = f"{downloaded_file_path}/{new_name}")
    
with DAG(
    dag_id = "s3_download", 
    schedule_interval = "@daily", 
    start_date = datetime(2024, 10, 8), 
    catchup = False
) as dag: 
    task_download_from_s3 = PythonOperator(
        task_id = "download_from_s3", 
        python_callable = download_from_s3, 
        op_kwargs = {
                'key': 'demo1.txt',
                'bucket_name': 'akankhya-sample-703403115',
                'local_path': 'demofolder/'
        }
    ) 
    
    task_rename_file = PythonOperator(
        task_id = 'rename_file',
        python_callable = rename_file, 
        op_kwargs = {
                'new_name': 's3_downloaded_demo1.txt'
        }
    )
    
    task_download_from_s3 >> task_rename_file