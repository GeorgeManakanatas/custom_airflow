from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from time import sleep
from random import seed, randint

default_args = {
    'owner': 'abc',
    'start_date': datetime(2020, 7, 1),
    'email': ['abc@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency':1,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def callable_thing():
    print('this is correct!')
    sleep(10)

def time_delay():
    seconds = 3
    print('delaying : ',seconds,' seconds')
    sleep(seconds)

def print_value_one():
    print('value one')
    sleep(10)

def print_value_zero():
    print('value zero')
    sleep(10)

def random_thing():
    seed(1)
    value = randint(0,1)
    print(' value is : ',value)
    if value==1:
        print_value_one
    elif value==0:
        print_value_zero
    else:
        print('strange value!')
    sleep(10)

dag = DAG('daily_processing', default_args=default_args)

task_1 = PythonOperator(
    task_id='print_the_context.',
    python_callable=callable_thing,
    dag=dag)

task_2 = PythonOperator(
    task_id='delay_3_secs..',
    python_callable=time_delay,
    dag=dag)

task_3 = PythonOperator(
    task_id='do_random_thing...',
    python_callable=random_thing,
    dag=dag)


task_1 >> task_2 >> task_3
