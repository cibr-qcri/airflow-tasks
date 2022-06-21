FROM mythril/myth
LABEL maintainer.0="CIBR-QCRI Team"

ARG eth_client_host=10.4.8.131
ARG eth_client_port=31389
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
ENV GREENPLUM_DB=$gp_db
ENV ETHEREUM_CLIENT_HOST=$eth_client_host
ENV ETHEREUM_CLIENT_PORT=$eth_client_port

RUN apt-get install -y libpq-dev python-dev

RUN pip3 install mythril psycopg2

COPY analyzer.py ./analyzer.py

ENTRYPOINT [ "python3", "analyzer.py" ]
