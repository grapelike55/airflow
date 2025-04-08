from airflow.models.dag import DAG
import datetime
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

import pendulum
from pprint import pprint


with DAG(
    dag_id="dags_bash_with_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    @task(task_id="python_task")
    def show_templates(**kwargs): 
        pprint(kwargs)
        
    show_templates()