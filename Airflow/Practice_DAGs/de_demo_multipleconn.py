# Library imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests

# User-defined functions for tasks
def print_welcome(): 
    print('Hello participants, Welcome to your first Airflow DAG!!!')
    
def print_date():
    print('Today is {}'.format(datetime.today().date()))
    
def print_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the day: "{}"'.format(quote))

def print_addition():
    x = 15
    y = 20
    z = x + y
    print("Addition of values in x and y are: ", z)

# DAG definition
dag = DAG(
            'de_demo_multipleconn',
            default_args = {'start_date': days_ago(1)}, 
            schedule_interval = '0 23 * * *', 
            catchup = False
)

# Task for welcome message printing
print_welcome_task = PythonOperator(
                        task_id = 'print_welcome', 
                        python_callable = print_welcome, 
                        dag = dag
) 

# Task for date printing
print_date_task = PythonOperator(
                        task_id = 'print_date', 
                        python_callable = print_date, 
                        dag = dag
)

# Task for quote printing
print_quote_task = PythonOperator(
                        task_id = 'print_quote', 
                        python_callable = print_quote, 
                        dag = dag
)

# Task for addition
print_addition_task = PythonOperator(
                        task_id = 'print_addition', 
                        python_callable = print_addition, 
                        dag = dag
)

# Task dependency
print_quote_task >> [print_welcome_task, print_addition_task] >> print_date_task 

