import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

def failure_end_job():
    print("BTC parsing job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

with models.DAG(
        dag_id='btc-parser-dag',
        schedule_interval='@weekly',
        default_args=default_dag_args) as dag:

    task_parser = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task",
        do_xcom_push=False,
        is_delete_operator_pod=False
    )

task_parser

