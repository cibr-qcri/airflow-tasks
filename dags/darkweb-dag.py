import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


YESTERDAY = datetime.datetime.now() + datetime.timedelta(days=1)

with models.DAG(
        dag_id='darkweb-hourly-dags',
        schedule_interval='@hourly',
        start_date=YESTERDAY) as dag:

    healthcheck_feed = KubernetesPodOperator(
        name="tor-healthcheck-domain-feed",
        image='toshiqcri/job-001:latest',
        image_pull_policy='Always',
        namespace='airflow-cluster',
        task_id="tor-healthcheck-domain-feed",
        do_xcom_push=False,
    )
