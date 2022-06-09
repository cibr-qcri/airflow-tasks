#!/bin/bash

PROVIDER_URI="http://"${BITCOIN_DAEMON_USERNAME}":"${BITCOIN_DAEMON_PASSWORD}"@"${BITCOIN_DAEMON_HOST}":"${BITCOIN_DAEMON_PORT}
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

echo "Staring BTC Parser ...."

purge_data() {
  rm transactions.json blocks.json enriched_transactions.json
  rm blocks_sql.csv tx_sql.csv in_addr_sql.csv out_addr_sql.csv
}

# apply database if not exists
if [[ "$( psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=gpadmin -c "SELECT 1 FROM pg_database WHERE datname='$GREENPLUM_DB'" | sed -n '3p' | xargs )" != '1' ]] ; then
  psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=gpadmin -f btc_blockchain_schema.sql
fi

file=data/last_processed_number-$START_BLOCK_HEIGHT-$END_BLOCK_HEIGHT.txt
if [ -e "$file" ]; then
    START_BLOCK_HEIGHT=$(< last_processed_number-$START_BLOCK_HEIGHT-$END_BLOCK_HEIGHT.txt)
else 
    echo "File last_processed_number-$START_BLOCK_HEIGHT-$END_BLOCK_HEIGHT.txt does not exist"
fi 

echo "Staring block height is $((START_BLOCK_HEIGHT))"

export start_block_height=${START_BLOCK_HEIGHT}
export end_block_height=${END_BLOCK_HEIGHT}
export parse_chunk=${BATCH_SIZE}

export_and_store_blocks() {
  local start_block_height=$1
  local end_block_height=$2

  bitcoinetl export_blocks_and_transactions --start-block "$((start_block_height))" --end-block "$((end_block_height))" \
        --provider-uri $PROVIDER_URI --batch-size 100 --chain bitcoin --blocks-output blocks.json --transactions-output transactions.json && \
  bitcoinetl enrich_transactions --provider-uri $PROVIDER_URI --batch-size 100 --transactions-input transactions.json \
        --transactions-output enriched_transactions.json  && \
  echo "Blocks exported from bitcoin-etl range $((start_block_height))-$((end_block_height))"
  python3 blockchain-parser.py && \
  psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_block(height, hash, block_time, tx_count) FROM blocks_sql.csv CSV DELIMITER E','"
  psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_transaction(hash, block_number, index, fee, input_value, output_value, is_coinbase, input_count, output_count, input_usd_value, output_usd_value, timestamp) FROM tx_sql.csv CSV DELIMITER E','"
  psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_input(tx_hash, address, address_type, tx_value, usd_value, block_number, timestamp) FROM in_addr_sql.csv CSV DELIMITER E','"
  psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_output(tx_hash, address, address_type, tx_value, usd_value, block_number, timestamp) FROM out_addr_sql.csv CSV DELIMITER E','"
  echo "Data successfully uploaded to GreenplumpDB from block range $((start_block_height))-$((end_block_height))"
  purge_data

  echo "$((end_block_height))" > data/last_processed_number-$START_BLOCK_HEIGHT-$END_BLOCK_HEIGHT.txt
}

while sleep 1; do
    block_count="$(bitcoin-cli -rpcconnect="$BITCOIN_DAEMON_HOST" -rpcuser="$BITCOIN_DAEMON_USERNAME" -rpcpassword="$BITCOIN_DAEMON_PASSWORD" -rpcport="$BITCOIN_DAEMON_PORT" getblockcount)"
    echo "Current block height - $((block_count))"
    if ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height >= start_block_height+parse_chunk)); then
        echo "Processing for block range $((start_block_height+1))-$((start_block_height+parse_chunk))"
        export_and_store_blocks "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export start_block_height=$((start_block_height+parse_chunk))
    elif ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height < start_block_height+parse_chunk)) && ((end_block_height > start_block_height)); then
        echo "Processing for block range $((start_block_height+1))-$((end_block_height))"
        export_and_store_blocks "$((start_block_height+1))" $((end_block_height))
        export start_block_height=$((end_block_height))
    elif ((end_block_height == start_block_height)); then
        echo "Parsing completed for the given upper bound block height - $((end_block_height))"
    fi
done

echo "Ending BTC Parser..."
