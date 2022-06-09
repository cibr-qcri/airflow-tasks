CREATE DATABASE IF NOT EXISTS btc_blockchain_new;

\c btc_blockchain_new;

CREATE TABLE IF NOT EXISTS btc_transaction (
	id SERIAL primary key NOT NULL,
	fee bigint,
	block_number integer,
	input_value bigint DEFAULT 0,
	index integer,
	is_coinbase boolean,
	output_count integer,
	output_value bigint,
	hash varchar(65),
	input_count integer,
	input_usd_value numeric,
	output_usd_value numeric,
	timestamp bigint
);

CREATE TABLE IF NOT EXISTS btc_block (
	id SERIAL primary key NOT NULL,
	height integer,
	hash varchar(65),
	block_time varchar(16),
	tx_count integer
);

CREATE TABLE IF NOT EXISTS btc_tx_input (
	id SERIAL primary key NOT NULL,
	tx_hash varchar(65),
	index integer,
	required_signatures integer,
	spent_output_index int,
	spent_tx_hash varchar(65),
	address_type varchar(16),
	tx_value bigint,
	address varchar(65),
	usd_value numeric,
	block_number integer,
	timestamp bigint
);

CREATE TABLE IF NOT EXISTS btc_tx_output (
	id SERIAL primary key NOT NULL,
	tx_hash varchar(65),
	index integer,
	required_signatures integer,
	address_type varchar(16),
	tx_value bigint,
	address varchar(65),
	usd_value numeric,
	block_number integer,
	timestamp bigint
);
