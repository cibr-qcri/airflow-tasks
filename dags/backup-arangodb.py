import datetime

from airflow import models
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = VolumeMount(
    'cibr-backup-mount',
    mount_path='/backups/arangodb/',
    sub_path=None,
    read_only=False
)

volume_config = {
    'name': 'cibr-backup-mount',
    'hostPath': {
        'path': '/cibr-dev-data/backups/arangodb'
    }
}
volume = Volume(name='cibr-backup-mount', configs=volume_config)


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
        dag_id='arangodb-backup-dag',
        schedule_interval='@weekly',
        default_args=default_dag_args) as dag:
    arangodb_backup = KubernetesPodOperator(
        namespace='airflow',
        name="arangodb_backup_dag",
        image='toshiqcri/backup-arangodb:latest',
        image_pull_policy='Always',
        task_id="arangodb_backup_dag",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True
    )

arangodb_backup
