from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from time import sleep
from random import seed, randint
import docker

default_args = {
    'owner': 'abc',
    'start_date': datetime(2021, 3, 11),
    'email': ['abc@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency':1,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def executable_function():
    # cli = docker.DockerClient(base_url='tcp://127.0.0.1:2375') # windows 
    cli = docker.DockerClient(base_url='unix://var/run/docker.sock') # linux
    containers = cli.containers.list(all=True)

    cont =containers[0]
    print(len(containers))
    print(cont.short_id)


dag = DAG('Docker_containers', default_args=default_args)

task_1 = PythonOperator(
    task_id='print_the_context.',
    python_callable=executable_function,
    dag=dag)


task_1