import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

with models.DAG(
        dag_id='darkweb-dags',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    k = KubernetesPodOperator(
        name="tor-healthcheck-domain-feed",
        image='toshiqcri/job-001:latest',
        image_pull_policy='Always',
        task_id="tor-healthcheck-domain-feed",
        do_xcom_push=True,
    )
