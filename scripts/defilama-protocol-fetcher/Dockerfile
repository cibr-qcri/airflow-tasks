FROM python:3.10

WORKDIR /usr/src/job

LABEL maintainer.0="CIBR-QCRI Team"

ARG gp_host=10.4.8.131
ARG gp_host_port=5432
ARG gp_username=gpadmin
ARG gp_password
ARG gp_db=eth_blockchain

ENV PYTHONUNBUFFERED=1
ENV GREENPLUM_HOST=$gp_host
ENV GREENPLUM_PORT=$gp_host_port
ENV GREENPLUM_USERNAME=$gp_username
ENV GREENPLUM_PASSWORD=$gp_password
ENV GREENPLUM_DEFI_DB=$gp_db

COPY requirements.txt ./
COPY data-retriever.py ./data-retriever.py
COPY dependencies/defi_protocol_schema.sql ./defi_protocol_schema.sql

RUN pip3 install -r requirements.txt

CMD [ "stdbuf", "-oL", "python3", "data-retriever.py" ]
