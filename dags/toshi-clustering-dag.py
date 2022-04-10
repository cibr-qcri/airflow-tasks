import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

def failure_end_job():
    print("Toshi clustering failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'on_failure_callback': failure_end_job
}

with models.DAG(
        dag_id='toshi-clustering-dag',
        schedule_interval='@daily',
        default_args=default_dag_args) as dag:

    task_clustering = KubernetesPodOperator(
        name="toshi_clustering_job",
        image='toshiqcri/job-002:latest',
        image_pull_policy='Always',
        namespace='airflow-cluster',
        task_id="toshi_clustering_job",
        do_xcom_push=False,
        is_delete_operator_pod=True
    )

task_clustering

