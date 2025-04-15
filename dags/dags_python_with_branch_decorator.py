from airflow.models.dag import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

import pendulum
import random

with DAG(
    dag_id="dags_python_with_branch_decorator",
    start_date=datetime(2025,4,1),
    schedule=None,
    catchup=False,
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_1st=['a','b','c']
        selected_item=random.choice(item_1st)
        if selected_item == 'a':
            return 'task_a'
        elif selected_item in ['b','c']:
            return ['task_b','task_c']
        
    def common_func(**kwargs):
        print(kwargs['selected'])
    
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'a'}
    )
    
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'b'}
    )
    
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'c'}
    )
    
    select_random() >> [task_a, task_b, task_c]
        
        