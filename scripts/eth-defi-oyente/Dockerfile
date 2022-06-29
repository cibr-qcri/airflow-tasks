FROM luongnguyen/oyente
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

RUN sed -i -e 's/archive.ubuntu.com\|security.ubuntu.com/old-releases.ubuntu.com/g' /etc/apt/sources.list
RUN apt-get -y dist-upgrade
RUN apt-get update
RUN apt-get install -y libpq-dev python-dev
RUN apt-get install -y python3-pip

RUN pip3 install psycopg2 py-etherscan-api

COPY analyzer.py ./analyzer.py

ENTRYPOINT [ "python3", "analyzer.py" ]
