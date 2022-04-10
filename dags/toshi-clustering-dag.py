import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = VolumeMount(
    'toshi-airflow-pvc',
    mount_path='/opt/airflow/dags',
    sub_path=None,
    read_only=False
)

volume_config = {
    'persistentVolumeClaim':{
        'claimName': 'toshi-airflow-pvc'
    }
}
volume = Volume(name='toshi-airflow-pvc', configs=volume_config)

def failure_end_job():
    print("Toshi clustering job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
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
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False
    )

task_clustering

