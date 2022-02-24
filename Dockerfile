FROM apache/airflow:2.0.2

USER root

COPY requirements.txt ./
COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/

RUN pip3 install -r requirements.txt

USER airflow