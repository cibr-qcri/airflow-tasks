FROM python:3.8

WORKDIR /usr/src/job

ARG modified_file

ARG elasticsearch_host
ARG elasticsearch_port
ARG redis_host
ARG redis_port
ARG gp_host
ARG gp_port
ARG gp_username
ARG gp_password
ARG gp_db

ENV PYTHONUNBUFFERED=1
ENV ES_CONNECTION_HOST=$elasticsearch_host
ENV ES_CONNECTION_PORT=$elasticsearch_port
ENV REDIS_CONNECTION_HOST=$redis_host
ENV REDIS_CONNECTION_PORT=$redis_port
ENV GREENPLUM_HOST=$gp_host
ENV GREENPLUM_PORT=$gp_host
ENV GREENPLUM_USERNAME=$gp_username
ENV GREENPLUM_PASSWORD=$gp_password
ENV GREENPLUM_DB=$gp_db

COPY requirements.txt ./

COPY $modified_file ./script.py

RUN pip3 install -r requirements.txt

CMD [ "python3", "script.py" ]
