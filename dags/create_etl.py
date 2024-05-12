from airflow import DAG 
from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta


default_args = {
    'owner' : 'John Doe',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'depends_on_past' : True
}



with DAG(
    dag_id='ingest_Colors_data_from_Landing_Zone',
    description='creates ETL pipeline for Colors Data Set',
    start_date=datetime(year=2022, month=12, day=21, hour=1, minute=30),
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    # task1 = BashOperator(task_id='greeting', bash_command='whoami')

    task2 = SSHOperator(
        task_id='remove_duplicate',
        ssh_conn_id='ssh_spark',
        command=f'''. /home/spark_user/docker_env.txt && spark-submit \
            --master spark://spark:7077 \
            --deploy-mode client \
            --executor-memory 4g \
            --executor-cores 2 \
            --driver-cores 1\
            --driver-memory 1g \
            --archives /opt/bitnami/spark/dist/lakehouse-0.1.tar.gz \
            /opt/bitnami/spark/dev/scripts/driver.py
            '''
    )

    # task3 = BashOperator(task_id='fin', bash_command='echo completed')

    task2
    # task1 >> task2 >> task3