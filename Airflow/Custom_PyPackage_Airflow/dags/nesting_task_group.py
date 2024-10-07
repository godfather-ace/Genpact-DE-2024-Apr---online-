from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
    dag_id = "nesting_task_group", 
    start_date = pendulum.datetime(2024, 10, 7, tz = 'utc'), 
    schedule_interval = None
) as dag: 
    start = EmptyOperator(task_id = "start")
    
    with TaskGroup(group_id = "group1") as tg1: 
        t1 = EmptyOperator(task_id = "task1")
        
        with TaskGroup(group_id = "group1_1") as tg1_1: 
            t2 = EmptyOperator(task_id = "task2")
            t3 = EmptyOperator(task_id = "task3")

            t2 >> t3
        
        with TaskGroup(group_id = "group1_2") as tg1_2: 
            for i in range(1, 3): 
                tn = EmptyOperator(task_id = f"task_tg1_2_{i}")
                
            t3 >> tn
            
        t1 >> tg1_1 >> tg1_2
        
    end = EmptyOperator(task_id = "end")
    start >> tg1 >> end