import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)


def failure_end_job():
    print("Defi fetcher failed")


default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

with models.DAG(
        dag_id='defi-fetcher-dag',
        schedule_interval='@weekly',
        default_args=default_dag_args) as dag:

    defilama_fetcher = KubernetesPodOperator(
        namespace='default',
        name='defilama_fetcher_task',
        image='toshiqcri/defilama-protocol-fetcher:latest',
        image_pull_policy='Always',
        task_id='defilama_fetcher_task',
        do_xcom_push=False,
        is_delete_operator_pod=True
    )

    coingecko_fetcher = KubernetesPodOperator(
        namespace='default',
        name='coingecko_fetcher_task',
        image='toshiqcri/coingecko-protocol-fetcher:latest',
        image_pull_policy='Always',
        task_id='coingecko_fetcher_task',
        do_xcom_push=False,
        is_delete_operator_pod=False
    )

    mythril_analyzer = KubernetesPodOperator(
        namespace='default',
        name='mythril_analyzer_task',
        image='toshiqcri/eth-defi-mythril:latest',
        image_pull_policy='Always',
        task_id='mythril_analyzer_task',
        do_xcom_push=False,
        is_delete_operator_pod=False
    )

defilama_fetcher >> coingecko_fetcher  >>  mythril_analyzer
