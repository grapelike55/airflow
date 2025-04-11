from airflow.models.dag import DAG
import datetime
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
) as dag:
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo START && "
                     "echo XCOM_PUSHED "
                     "{{ ti.     
    )