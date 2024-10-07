from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.utils.dates import days_ago
from datetime import datetime

def machine_learning_task():
    # importing the libraries
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn import preprocessing
    import seaborn as sns
    
    # loading the iris dataset
    df = sns.load_dataset('iris')
        
    # encoding categorical variable - species
    label_encoder = preprocessing.LabelEncoder()
    df['species'] = label_encoder.fit_transform(df['species'])
    df.head()
    
    # X and y variables
    X = df[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
    y = df['species']

    # model training and accuracy
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)
    model = LogisticRegression()
    model.fit(X_train, y_train)
    accuracy = model.score(X_test, y_test)
    print("Accuracy: ", accuracy)
    
# DAG definition
with DAG(
    dag_id = "ml_demo_dag", 
    description = "DAG for simple ml model",
    default_args = {'start_date': days_ago(1)}, 
    schedule_interval = '0 23 * * *', 
    catchup = False
) as dag:
    
    # tasks for dag 
    task_bash = BashOperator(
                    task_id = "bash_task",
                    bash_command = """
                                echo "Executing the dag file"
                                """  
    ) 
    
    task_python = PythonOperator(
                    task_id = "ml_using_python",
                    python_callable = machine_learning_task
    )
    
    # task dependency settings
    task_bash >> task_python