CREATE DATABASE eth_blockchain;
\c eth_blockchain;

CREATE TABLE eth_block (
	id SERIAL primary key NOT NULL,
    number bigint,
    hash varchar(256),
    parent_hash	varchar(256),
    nonce varchar(256),
    sha3_uncles	varchar(256),
    logs_bloom text,
    transactions_root varchar(256),
    state_root varchar(256),
    receipts_root varchar(256),
    miner varchar(256),
    difficulty numeric,
    total_difficulty numeric,
    size bigint,
    extra_data text,
    gas_limit bigint,
    gas_used bigint,
    timestamp bigint,
    transaction_count bigint,
    base_fee_per_gas bigint
);

CREATE TABLE eth_transaction (
    id SERIAL primary key NOT NULL,
    hash varchar(256),
    nonce bigint,
    block_hash varchar(256),
    block_number bigint,
    transaction_index bigint,
    from_address varchar(256),
    to_address varchar(256),
    value numeric,
    gas bigint,
    gas_price bigint,
    input text,
    block_timestamp bigint,
    max_fee_per_gas bigint,
    max_priority_fee_per_gas bigint,
    transaction_type bigint
);

CREATE TABLE eth_token_transfer (
    id SERIAL primary key NOT NULL,
    token_address varchar(256),
    from_address varchar(256),
    to_address varchar(256),
    value numeric,
    transaction_hash varchar(256),
    log_index bigint,
    block_number bigint
);

CREATE TABLE eth_receipt (
    id SERIAL primary key NOT NULL,
    transaction_hash varchar(256),
    transaction_index bigint,
    block_hash varchar(256),
    block_number bigint,
    cumulative_gas_used bigint,
    gas_used bigint,
    contract_address varchar(256),
    root varchar(256),
    status bigint,
    effective_gas_price bigint
);

CREATE TABLE eth_log (
    id SERIAL primary key NOT NULL,
    log_index bigint,
    transaction_hash varchar(256),
    transaction_index bigint,
    block_hash varchar(256),
    block_number bigint,
    address varchar(256),
    data text,
    topics text
);

CREATE TABLE eth_contract (
    id SERIAL primary key NOT NULL,
    address	varchar(256),
    bytecode text,
    function_sighashes text,
    is_erc20 boolean,
    is_erc721 boolean,
    block_number bigint
);

CREATE TABLE eth_token (
    id SERIAL primary key NOT NULL,
    address	varchar(256),
    symbol text,
    name text,
    decimals bigint,
    total_supply numeric
);

CREATE TABLE eth_trace (
    id SERIAL primary key NOT NULL,
    block_number bigint,
    transaction_hash varchar(256),
    transaction_index bigint,
    from_address varchar(256),
    to_address varchar(256),
    value numeric,
    input text,
    output text,
    trace_type varchar(256),
    call_type varchar(256),
    reward_type varchar(256),
    gas bigint,
    gas_used bigint,
    subtraces bigint,
    trace_address varchar(256),
    error text,
    status bigint,
    trace_id text
);
