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

def failure_end_job():
    print("BTC address labeling job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

with models.DAG(
        dag_id='btc-address-labeling-dag',
        schedule_interval='@monthly',
        start_date=YESTERDAY) as dag:

    task_address_labeling = KubernetesPodOperator(
        name="btc_address_labeling_job",
        image='toshiqcri/btc-address-labels:latest',
        image_pull_policy='Always',
        task_id="btc_address_labeling_job",
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=False,
    )

task_address_labeling
