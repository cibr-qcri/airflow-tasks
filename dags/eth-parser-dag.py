import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = VolumeMount(
    'cibr-airflow-pvc',
    mount_path='/blockchain-parser/data/',
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
    print("ETH parsing job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

env_vars_1 = {
    'START_BLOCK_HEIGHT' : '6550000',
    'END_BLOCK_HEIGHT' : '8000000',
    'BATCH_SIZE': '10000',
    'ETHEREUM_CLIENT_HOST': '10.4.8.131',
    'ETHEREUM_CLIENT_PORT': '31389'
}

env_vars_2 = {
    'START_BLOCK_HEIGHT' : '8180000',
    'END_BLOCK_HEIGHT' : '10000000',
    'BATCH_SIZE': '10000',
    'ETHEREUM_CLIENT_HOST': '10.4.8.131',
    'ETHEREUM_CLIENT_PORT': '31390'
}

env_vars_3 = {
    'START_BLOCK_HEIGHT' : '10160000',
    'END_BLOCK_HEIGHT' : '12000000',
    'BATCH_SIZE': '10000',
    'ETHEREUM_CLIENT_HOST': '10.4.8.131',
    'ETHEREUM_CLIENT_PORT': '31391'
}

env_vars_4 = {
    'START_BLOCK_HEIGHT' : '12140000',
    'END_BLOCK_HEIGHT' : '14000000',
    'BATCH_SIZE': '10000',
    'ETHEREUM_CLIENT_HOST': '10.4.8.131',
    'ETHEREUM_CLIENT_PORT': '31392'
}

env_vars_5 = {
    'START_BLOCK_HEIGHT' : '14120000',
    'END_BLOCK_HEIGHT' : '16000000',
    'BATCH_SIZE': '10000',
    'ETHEREUM_CLIENT_HOST': '10.4.8.131',
    'ETHEREUM_CLIENT_PORT': '31389'
}

with models.DAG(
        dag_id='eth-parser-dag',
        schedule_interval=None,
        start_date=YESTERDAY) as dag:

    task_parser_1 = KubernetesPodOperator(
        namespace='default',
        name="eth_parsing_task_6000000-8000000",
        image='toshiqcri/eth-etl-parser:latest',
        image_pull_policy='Always',
        task_id="eth_parsing_task_6000000-8000000",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_1,
        is_delete_operator_pod=False
    )

    task_parser_2 = KubernetesPodOperator(
        namespace='default',
        name="eth_parsing_task_8000000-10000000",
        image='toshiqcri/eth-etl-parser:latest',
        image_pull_policy='Always',
        task_id="eth_parsing_task_8000000-10000000",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_2,
        is_delete_operator_pod=False
    )

    task_parser_3 = KubernetesPodOperator(
        namespace='default',
        name="eth_parsing_task_10000000-12000000",
        image='toshiqcri/eth-etl-parser:latest',
        image_pull_policy='Always',
        task_id="eth_parsing_task_10000000-12000000",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_3,
        is_delete_operator_pod=False
    )

    task_parser_4 = KubernetesPodOperator(
        namespace='default',
        name="eth_parsing_task_12000000-14000000",
        image='toshiqcri/eth-etl-parser:latest',
        image_pull_policy='Always',
        task_id="eth_parsing_task_12000000-14000000",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_4,
        is_delete_operator_pod=False
    )

    task_parser_5 = KubernetesPodOperator(
        namespace='default',
        name="eth_parsing_task_14000000-16000000",
        image='toshiqcri/eth-etl-parser:latest',
        image_pull_policy='Always',
        task_id="eth_parsing_task_14000000-16000000",
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_5,
        is_delete_operator_pod=False
    )

[task_parser_1, task_parser_2, task_parser_3, task_parser_4, task_parser_5]

