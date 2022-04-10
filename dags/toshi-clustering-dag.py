import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
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
    )

task_clustering
