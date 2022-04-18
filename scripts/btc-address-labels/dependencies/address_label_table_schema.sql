CREATE TABLE IF NOT EXISTS btc_address_label (
	id SERIAL primary key NOT NULL,
	cluster_id varchar(65),
	address varchar(65),
	label text,
	source varchar(255),
	category varchar(255),
	note text,
	timestamp bigint
);

