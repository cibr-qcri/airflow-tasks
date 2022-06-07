#!/bin/bash

export PROVIDER_URI="http://"${ETHEREUM_CLIENT_HOST}":"${ETHEREUM_CLIENT_PORT}
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

echo "Ethereum Parser Staring ...."

# apply database if not exists
if [[ "$( psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=$GREENPLUM_USERNAME -c "SELECT 1 FROM pg_database WHERE datname='$GREENPLUM_DB'" )" != '1' ]] ; then
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=$GREENPLUM_USERNAME -f /eth_blockchain_schema.sql
fi

# find last inserted block height for the given range
LAST_BLOCK_HEIGHT=$(psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "SELECT max(number) FROM eth_block WHERE number >= $START_BLOCK_HEIGHT and number <= $END_BLOCK_HEIGHT;" | sed -n '3p' | xargs)
re='^[0-9]+$'
if [[ $LAST_BLOCK_HEIGHT =~ $re ]] ; then
   START_BLOC_HEIGHTK=$LAST_BLOCK_HEIGHT
fi

export_blocks_and_transactions() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Blocks and Txes --"
  ethereumetl export_blocks_and_transactions --start-block "$((start_block_height))" --end-block "$((end_block_height))" --provider-uri $PROVIDER_URI --blocks-output blocks.csv --transactions-output transactions.csv
  echo "Blocks and Txes exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
}

export_token_transfers() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Token transferes --"
  ethereumetl export_token_transfers --start-block "$((start_block_height))" --end-block "$((end_block_height))" --provider-uri $PROVIDER_URI --output token_transfers.csv
  echo "Token transferes exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
}

export_receipts_and_logs() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Receipts and logs --"
  ethereumetl extract_csv_column --input transactions.csv --column hash --output transaction_hashes.txt
  ethereumetl export_receipts_and_logs --transaction-hashes transaction_hashes.txt --provider-uri $PROVIDER_URI --receipts-output receipts.csv --logs-output logs.csv
  echo "Receipts and logs exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
}

export_contracts() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Contracts --"
  ethereumetl extract_csv_column --input receipts.csv --column contract_address --output contract_addresses.txt
  ethereumetl export_contracts --contract-addresses contract_addresses.txt --provider-uri $PROVIDER_URI --output contracts.csv
  echo "Contracts exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
}

export_tokens() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Tokens --"
  ethereumetl filter_items -i contracts.csv -p "item['is_erc20'] or item['is_erc721']" | \
  ethereumetl extract_field -f address -o token_addresses.txt
  ethereumetl export_tokens --token-addresses token_addresses.txt --provider-uri $PROVIDER_URI --output tokens.csv
  echo "Tokens exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
  echo "Data successfully uploaded to GreenplumpDB from block range $((start_block_height))-$((end_block_height))"
}

export_traces() {
  local start_block_height=$1
  local end_block_height=$2
  echo "-- Traces --"
  ethereumetl export_geth_traces --start-block "$((start_block_height))" --end-block "$((end_block_height))" --provider-uri $PROVIDER_URI --output geth_traces.json
  ethereumetl extract_geth_traces --input geth_traces.json --output traces.csv
  echo "Traces exported from ethereum-etl range $((start_block_height))-$((end_block_height))"
  echo "Data successfully uploaded to GreenplumpDB from block range $((start_block_height))-$((end_block_height))"
}

export_data() {
  sed -i '1d' blocks.csv
  sed -i '1d' transactions.csv
  sed -i '1d' token_transfers.csv
  sed -i '1d' receipts.csv
  sed -i '1d' logs.csv
  sed -i '1d' contracts.csv
  sed -i '1d' tokens.csv

  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_block(number,hash,parent_hash,nonce,sha3_uncles,logs_bloom,transactions_root,state_root,receipts_root,miner,difficulty,total_difficulty,size,extra_data,gas_limit,gas_used,timestamp,transaction_count,base_fee_per_gas) FROM blocks.csv CSV DELIMITER E','"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_transaction(hash, block_number, index, fee, input_value, output_value, is_coinbase, input_count, output_count, input_usd_value, output_usd_value, timestamp) FROM transactions.csv CSV DELIMITER E','"
  echo "Blocks and Txes data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_token_transfer(token_address,from_address,to_address,value,transaction_hash,log_index,block_number) FROM token_transfers.csv CSV DELIMITER E','"
  echo "Token transfer data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_receipt(transaction_hash,transaction_index,block_hash,block_number,cumulative_gas_used,gas_used,contract_address,root,status,effective_gas_price) FROM receipts.csv CSV DELIMITER E','"
  echo "Receipts data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_log(log_index,transaction_hash,transaction_index,block_hash,block_number,address,data,topics) FROM logs.csv CSV DELIMITER E','"
  echo "Logs data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_contract(address,bytecode,function_sighashes,is_erc20,is_erc721,block_number) FROM contracts.csv CSV DELIMITER E','"
  echo "Contract data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
  psql -h "$GREENPLUM_HOST" -p "$GREENPLUM_SERVICE_PORT" -d "$GREENPLUM_DB" --user=$GREENPLUM_USERNAME -c "\\COPY eth_token(address,symbol,name,decimals,total_supply,block_number) FROM tokens.csv CSV DELIMITER E','"
  echo "Tokens data successfully uploaded to the GreenplumpDB for block range $((start_block_height))-$((end_block_height))"
}

purge_csv() {
  rm blocks.csv transactions.csv token_transfers.csv receipts.csv logs.csv token_transfers.csv tokens.csv
}

echo "Staring block height is $((START_BLOCK_HEIGHT))"
export start_block_height=${START_BLOCK_HEIGHT}
export end_block_height=${END_BLOCK_HEIGHT}
export parse_chunk=${BATCH_SIZE}

while sleep 1; do
    block_count=$(printf "%d\n" $(curl --silent -H "Content-Type: application/json"  -X POST --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' "$((PROVIDER_URI))" | jq '.result.currentBlock' | tr -d '"' ))
    echo "Current block height - $((block_count))"

    if ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height >= start_block_height+parse_chunk)); then
        echo "Processing for block range $((start_block_height+1))-$((start_block_height+parse_chunk))"
        export_blocks_and_transactions "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export_token_transfers "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export_receipts_and_logs "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export_contracts "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export_tokens "$((start_block_height+1))" $((start_block_height+parse_chunk))
        export_data
        purge_csv
        export start_block_height=$((start_block_height+parse_chunk))
    elif ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height < start_block_height+parse_chunk)) && ((end_block_height > start_block_height)); then
        echo "Processing for block range $((start_block_height+1))-$((end_block_height))"
        export_blocks_and_transactions "$((start_block_height+1))" $((end_block_height))
        export_token_transfers "$((start_block_height+1))" $((end_block_height))
        export_receipts_and_logs "$((start_block_height+1))" $((end_block_height))
        export_contracts "$((start_block_height+1))" $((end_block_height))
        export_tokens "$((start_block_height+1))" $((end_block_height))
        export_data
        purge_csv
        export start_block_height=$((end_block_height))
    elif ((end_block_height == start_block_height)); then
        printf "Current date and time in Linux %s\n" "$(date)"
        echo "Parsing completed for the given upper bound block height - $((end_block_height))"
    fi
done

echo "Ethereum Parser Ending ...."
