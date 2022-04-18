FROM python:3.10

LABEL maintainer.0="CIBR-QCRI Team"

RUN pip3 install python-arango

ARG gp_host
ARG gp_port
ARG gp_username
ARG gp_password
ARG gp_db
ARG arango_host
ARG arango_port
ARG arango_username
ARG arango_password
ARG arango_db

ENV BITCOIN_VERSION=0.18.1
ENV BITCOIN_DAEMON_HOST=127.0.0.1
ENV BITCOIN_DAEMON_USERNAME=user
ENV BITCOIN_DAEMON_PASSWORD=password
ENV BITCOIN_DAEMON_PORT=8332
ENV PATH=/opt/bitcoin-${BITCOIN_VERSION}/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV GREENPLUM_HOST=$gp_host
ENV GREENPLUM_PORT=$gp_port
ENV GREENPLUM_USERNAME=$gp_username
ENV GREENPLUM_PASSWORD=$gp_password
ENV GREENPLUM_DB=$gp_db
ENV ARANGO_USERNAME=$arango_username
ENV ARANGO_PASSWORD=$arango_password
ENV ARANGO_HOST=$arango_host
ENV ARANGO_PORT=$arango_port
ENV ARANGO_DB=$arango_db

WORKDIR "/blockchain-parser"
COPY score.py /score.py

EXPOSE 8332 8333 18332 18333 18443 18444 5432 8529

ENTRYPOINT ["/score.py"]
