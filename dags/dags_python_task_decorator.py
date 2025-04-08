from airflow.models.dag import DAG
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp
from airflow.decorators import dag, task
from pprint import pprint

import pendulum
import random

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context("task_decorator 실행")
    