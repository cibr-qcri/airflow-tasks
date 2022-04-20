FROM arangodb/arangodb:3.9.1
LABEL maintainer.0="CIBR-QCRI Team"

ARG arango_host
ARG arango_port
ARG arango_username

ENV ARANGO_HOST=$arango_host
ENV ARANGO_PORT=$arango_port
ENV ARANGO_USERNAME=$arango_username

COPY backup_arango.sh ./backup_arango.sh

CMD ["./backup_arango.sh"]
