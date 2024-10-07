from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
    dag_id = "task_group_simple", 
    start_date = pendulum.datetime(2024, 10, 7, tz = 'utc'), 
    schedule_interval = None
) as dag: 
    start = EmptyOperator(task_id = "start")
    
    with TaskGroup(group_id = "group1") as tg1: 
        t1 = EmptyOperator(task_id = "task1")
        t2 = EmptyOperator(task_id = "task2")
        
        t1 >> t2
        
    end = EmptyOperator(task_id = "end")
    start >> tg1 >> end