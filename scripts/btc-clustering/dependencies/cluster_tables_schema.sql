CREATE TABLE tmp_btc_address_cluster (
	id SERIAL primary key NOT NULL,
	cluster_id varchar(65),
	address varchar(65),
	total_spent_satoshi bigint,
	total_spent_usd numeric,
	total_received_satoshi bigint,
	total_received_usd numeric
);

CREATE TABLE tmp_btc_wallet(
	id SERIAL primary key NOT NULL,
	cluster_id varchar(65),
	num_address integer,
	num_tx integer,
	total_spent bigint,
	total_spent_usd numeric,
	total_received bigint,
	total_received_usd numeric,
	risk_score float DEFAULT -1,
	labels text,
	categories text
);

CREATE TABLE tmp_btc_wallet_transaction(
	id SERIAL primary key NOT NULL,
	cluster_id varchar(100),
	tx_hash varchar(65),
	block_number integer,
	input_value bigint,
	output_value bigint,
	is_coinbase boolean,
	input_count integer,
	output_count integer,
	tx_type varchar(40),
	input_usd_value numeric,
	output_usd_value numeric
);
