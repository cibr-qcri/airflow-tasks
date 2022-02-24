FROM apache/airflow:2.0.2

USER root

RUN pip3 install -r requirements.txt

COPY requirements.txt ./
COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/

USER airflow