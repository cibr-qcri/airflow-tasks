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
    print("BTC parsing job failed")

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': failure_end_job
}

affinity_1 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl012"]
                }
            ]
          }
        }
      ]
    }
}

affinity_2 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl027"]
                }
            ]
          }
        }
      ]
    }
}

affinity_3 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl023"]
                }
            ]
          }
        }
      ]
    }
}

affinity_4 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl024"]
                }
            ]
          }
        }
      ]
    }
}

affinity_5 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl025"]
                }
            ]
          }
        }
      ]
    }
}

affinity_6 = {
    "nodeAffinity": {
      "preferredDuringSchedulingIgnoredDuringExecution": [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": [
                {
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": ["cybubtoshicl026"]
                }
            ]
          }
        }
      ]
    }
}

# env_vars_1 = {
#     'START_BLOCK_HEIGHT' : '0',
#     'END_BLOCK_HEIGHT' : '200000',
#     'BATCH_SIZE': '10000',
#     'BITCOIN_DAEMON_HOST': '10.4.8.146',
#     'BITCOIN_DAEMON_PORT': '30239'
# }

env_vars_2 = {
    'START_BLOCK_HEIGHT' : '200000',
    'END_BLOCK_HEIGHT' : '300000',
    'BATCH_SIZE': '1000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30234'
}

env_vars_3 = {
    'START_BLOCK_HEIGHT' : '300000',
    'END_BLOCK_HEIGHT' : '400000',
    'BATCH_SIZE': '1000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30235'
}

env_vars_4 = {
    'START_BLOCK_HEIGHT' : '400000',
    'END_BLOCK_HEIGHT' : '500000',
    'BATCH_SIZE': '10000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30236'
}

env_vars_5 = {
    'START_BLOCK_HEIGHT' : '500000',
    'END_BLOCK_HEIGHT' : '600000',
    'BATCH_SIZE': '1000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30237'
}

env_vars_6 = {
    'START_BLOCK_HEIGHT' : '600000',
    'END_BLOCK_HEIGHT' : '700000',
    'BATCH_SIZE': '1000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30238'
}

env_vars_7 = {
    'START_BLOCK_HEIGHT' : '700000',
    'END_BLOCK_HEIGHT' : '741900',
    'BATCH_SIZE': '1000',
    'BITCOIN_DAEMON_HOST': '10.4.8.146',
    'BITCOIN_DAEMON_PORT': '30239'
}

with models.DAG(
        dag_id='btc-parser-dag',
        schedule_interval='@weekly',
        default_args=default_dag_args) as dag:

    # task_parser_1 = KubernetesPodOperator(
    #     namespace='default',
    #     name="btc_parsing_task_1-200000",
    #     image='toshiqcri/btc-etl-parser:latest',
    #     image_pull_policy='Always',
    #     task_id="btc_parsing_task_1-200000",
    #     do_xcom_push=False,
    #     is_delete_operator_pod=False,
    #     volumes=[volume],
    #     volume_mounts=[volume_mount],
    #     env_vars = env_vars_1
    # )

    task_parser_2 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_200000-300000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_200000-300000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_2,
        affinity=affinity_1
    )

    task_parser_3 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_300000-400000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_300000-400000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_3,
        affinity=affinity_2
    )

    task_parser_4 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_400000-500000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_400000-500000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_4,
        affinity=affinity_3
    )

    task_parser_5 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_500000-600000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_500000-600000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_5,
        affinity=affinity_4
    )

    task_parser_6 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_600000-700000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_600000-700000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars = env_vars_6,
        affinity=affinity_5
    )

    task_parser_7 = KubernetesPodOperator(
        namespace='default',
        name="btc_parsing_task_700000-800000",
        image='toshiqcri/btc-etl-parser:latest',
        image_pull_policy='Always',
        task_id="btc_parsing_task_700000-800000",
        do_xcom_push=False,
        is_delete_operator_pod=False,
        env_vars = env_vars_7,
        affinity=affinity_6
    )

[task_parser_2, task_parser_3, task_parser_4, task_parser_5, task_parser_6, task_parser_7]
