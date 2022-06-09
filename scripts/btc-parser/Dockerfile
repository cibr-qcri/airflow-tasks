FROM python:3.10

LABEL maintainer.0="CIBR-QCRI Team"

RUN useradd -r bitcoin \
  && apt-get update -y \
  && apt-get install -y curl gnupg gosu python3-pip postgresql-client\
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* 

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
ENV GREENPLUM_HOST=10.4.8.146
ENV GREENPLUM_PORT=$gp_port
ENV GREENPLUM_USERNAME=$gp_username
ENV GREENPLUM_PASSWORD=$gp_password
ENV GREENPLUM_DB=btc_blockchain_new
ENV ARANGO_USERNAME=$arango_username
ENV ARANGO_PASSWORD=$arango_password
ENV ARANGO_HOST=10.4.8.146
ENV ARANGO_PORT=$arango_port
ENV ARANGO_DB=$arango_db

RUN set -ex \
  && curl -SLO https://bitcoin.org/bin/bitcoin-core-${BITCOIN_VERSION}/bitcoin-${BITCOIN_VERSION}-x86_64-linux-gnu.tar.gz \
  && tar -xzf *.tar.gz -C /opt \
  && rm *.tar.gz

RUN mkdir /blockchain-parser
WORKDIR "/blockchain-parser"
COPY bootstrap.sh /bootstrap.sh
COPY blockchain-parser.py /blockchain-parser.py
COPY dependencies/btc_blockchain_schema.sql /btc_blockchain_schema.sql

RUN pip3 install bitcoin-etl python-arango

RUN chmod 755 /bootstrap.sh
RUN chmod 755 /blockchain-parser.py
RUN chmod 755 /btc_blockchain_schema.sql

EXPOSE 8332 8333 18332 18333 18443 18444 5432 8529

ENTRYPOINT ["/bootstrap.sh"]
