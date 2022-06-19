#!/bin/bash

export PROVIDER_URI="http://"${ETHEREUM_CLIENT_HOST}":"${ETHEREUM_CLIENT_PORT}
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# apply database if not exists
if [[ "$( psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=$GREENPLUM_USERNAME -c "SELECT 1 FROM pg_database WHERE datname='$GREENPLUM_DB'" | sed -n '3p' | xargs )" != '1' ]] ; then
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=$GREENPLUM_USERNAME -f /eth_blockchain_schema.sql
fi
