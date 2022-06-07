import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = VolumeMount(
    'cibr-airflow-pvc',
    mount_path='/opt/airflow/dags/',
    sub_path=None,
    read_only=False
)

volume_config = {
    'persistentVolumeClaim':{
        'claimName': 'cibr-airflow-pvc'
    }
}
volume = Volume(name='cibr-airflow-pvc', configs=volume_config)

resources = { 
    "request_cpu" :"4", 
    "request_memory": "8G", 
    "limit_cpu": "4", 
    "limit_memory": "60G"
}

def failure_end_job():
    print("BTC wallet clustering job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

with models.DAG(
        dag_id='btc-clustering-dag',
        schedule_interval='@daily',
        default_args=default_dag_args) as dag:

    task_clustering = KubernetesPodOperator(
        namespace='airflow',
        name="btc_clustering_job",
        image='toshiqcri/clustering-task-01:latest',
        image_pull_policy='Always',
        task_id="btc_clustering_job",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False
    )

    task_cluster_mapping = KubernetesPodOperator(
        namespace='airflow',
        name="btc_cluster_mapping_job",
        image='toshiqcri/clustering-task-02:latest',
        image_pull_policy='Always',
        task_id="btc_cluster_mapping_job",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False
    )

    task_enrich_tables = KubernetesPodOperator(
        namespace='airflow',
        name="btc_enrich_tables_job",
        image='toshiqcri/clustering-task-03:latest',
        image_pull_policy='Always',
        task_id="btc_enrich_tables_job",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False
    )

    task_link_wallet_tables = KubernetesPodOperator(
        namespace='airflow',
        name="btc_link_wallet_tables_job",
        image='toshiqcri/clustering-task-04:latest',
        image_pull_policy='Always',
        task_id="btc_link_wallet_tables_job",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False
    )

task_clustering >> task_cluster_mapping >> task_enrich_tables >> task_link_wallet_tables
