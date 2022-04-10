FROM python:3.8

WORKDIR /opt/airflow/dags

ARG modified_file

ARG elasticsearch_host
ARG elasticsearch_port
ARG redis_host
ARG redis_port

ENV PYTHONUNBUFFERED=1
ENV ES_CONNECTION_HOST=$elasticsearch_host
ENV ES_CONNECTION_PORT=$elasticsearch_port
ENV REDIS_CONNECTION_HOST=$redis_host
ENV REDIS_CONNECTION_PORT=$redis_port

COPY requirements.txt ./

COPY $modified_file ./script.py

RUN pip3 install -r requirements.txt

CMD [ "python3", "script.py" ]