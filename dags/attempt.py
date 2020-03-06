from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'abc',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['abc@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def callable_thing():
    print('this is correct!')

dag = DAG('daily_processing', default_args=default_args)

task_1 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=callable_thing,
    dag=dag)