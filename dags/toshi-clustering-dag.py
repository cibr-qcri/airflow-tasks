import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

def start_job():
    print("Toshi clustering started")

def success_end_job():
    print("Toshi clustering finished successfully")

def failure_end_job():
    print("Toshi clustering failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'on_failure_callback': failure_end_job,
}

with models.DAG(
        dag_id='toshi-clustering-dag',
        schedule_interval='@daily',
        default_args=default_dag_args) as dag:

    task_start = PythonOperator(
        task_id="start_job",
        python_callable=start_job)

    task_clustering = KubernetesPodOperator(
        name="toshi_clustering_job",
        image='toshiqcri/job-002:latest',
        image_pull_policy='Always',
        namespace='airflow-cluster',
        task_id="toshi_clustering_job",
        do_xcom_push=False,
        is_delete_operator_pod=True
    )

    task_success_end = PythonOperator(
        task_id="success_end_job",
        python_callable=success_end_job)

    task_failure_end = PythonOperator(
        task_id="failure_end_job",
        python_callable=failure_end_job)

task_start >> task_clustering >> task_success_end
