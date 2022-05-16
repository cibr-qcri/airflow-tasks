CREATE TABLE tmp_btc_address_cluster (
	id SERIAL primary key NOT NULL,
	cluster_id varchar(65),
	address varchar(65),
	total_spent_satoshi bigint DEFAULT 0,
	total_spent_usd numeric DEFAULT 0,
	total_received_satoshi bigint DEFAULT 0,
	total_received_usd numeric DEFAULT 0
);

CREATE TABLE tmp_btc_wallet(
	id SERIAL primary key NOT NULL,
	cluster_id varchar(65),
	num_address integer DEFAULT 0,
	num_tx integer DEFAULT 0,
	total_spent bigint DEFAULT 0,
	total_spent_usd numeric DEFAULT 0,
	total_received bigint DEFAULT 0,
	total_received_usd numeric DEFAULT 0,
	risk_score float DEFAULT -1,
	label text,
	category text
);

CREATE TABLE tmp_btc_wallet_transaction(
	id SERIAL primary key NOT NULL,
	cluster_id varchar(100),
	tx_hash varchar(65),
	block_number integer,
	input_value bigint  DEFAULT 0,
	output_value bigint  DEFAULT 0,
	is_coinbase boolean,
	input_count integer  DEFAULT 0,
	output_count integer  DEFAULT 0,
	tx_type varchar(40),
	input_usd_value numeric DEFAULT 0,
	output_usd_value numeric DEFAULT 0,
	timestamp bigint
);

CREATE TABLE tmp_btc_link_wallet (
	id SERIAL primary key NOT NULL,
	first_cluster varchar(65),
  	second_cluster varchar(65),
	num_txes_to_second integer DEFAULT 0,
  	num_txes_from_second integer DEFAULT 0,
	total_satoshi_to_second bigint DEFAULT 0,
	total_usd_to_second numeric DEFAULT 0,
	total_satoshi_from_second bigint DEFAULT 0,
	total_usd_from_second numeric DEFAULT 0,
	first_cluster_size integer DEFAULT 0,
  	second_cluster_size integer DEFAULT 0
);
