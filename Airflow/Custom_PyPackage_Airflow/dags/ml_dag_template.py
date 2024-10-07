from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore

# Define your data preprocessing function
def preprocess_data():
    # Add your data preprocessing code here
    print("Data preprocessing completed.")

# Define your model training function
def train_model():
    # Add your model training code here
    print("Model training completed.")

# Define your model evaluation function
def evaluate_model():
    # Add your model evaluation code here
    print("Model evaluation completed.")

# Define DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 7),
    'email': ['xyz@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'ml_dag_template',
    default_args=default_args,
    description='Machine Learning Pipeline',
    schedule_interval=timedelta(days=1),  # Run daily
)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)
preprocess_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)
train_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)
evaluate_task = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag,
)
end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >> preprocess_task >> train_task >> evaluate_task >> end_task 