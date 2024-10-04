import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

def uploadmongo(ti, **context):
    try: 
        hook = MongoHook(mongo_conn_id = 'mongoid')
        client = hook.get_conn()
        db = client.Mydb
        sample_collection = db.sample_collection
        print(f'Connected to MongoDB - {client.server_info()}')
        d = json.loads(context['result'])
        sample_collection.insert_one(d)
    except Exception as e: 
        print("Error")
        
with DAG(
    dag_id = 'de_mongo_airflow', 
    schedule_interval = None, 
    start_date = datetime(2024-4-12), 
    catchup = False, 
    tags = ['sample'], 
    default_args ={
                "owner": "Sachin",
                "retries": 2,
                "retry_delay": timedelta(minutes = 2),
                "on_failure_callback": on_failure_callback
            }
) as dag:
    
    t1 = SimpleHttpOperator(
        task_id = 'get_sample', 
        method = 'GET', 
        endpoint = '2024-01-01..2024-06-30', 
        do_xcom_push = True, 
        dag = dag
    ) 
    
    t2 = PythonOperator(
            task_id = 'upload_mongodb', 
            python_callable = uploadmongo, 
            op_kwargs = {'result': t1.output}, 
            dag = dag
    )

t1 >> t2    
        