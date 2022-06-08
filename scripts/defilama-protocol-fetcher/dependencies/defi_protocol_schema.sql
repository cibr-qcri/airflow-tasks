
CREATE TABLE IF NOT EXISTS defilama_protocol (
    id varchar primary key NOT NULL,
	symbol varchar,
	name varchar,
	address varchar,
	url varchar NULL,
	description varchar NULL,
	chain varchar,
	gecko_id varchar,
	cmc_id varchar,
	twitter varchar(16) NULL,
	category varchar NULL,
	chains varchar array NULL
);

CREATE TABLE IF NOT EXISTS temporary_table (
    id varchar,
	symbol varchar,
	name varchar,
	address varchar,
	url varchar NULL,
	description varchar NULL,
	chain varchar,
	gecko_id varchar,
	cmc_id varchar,
	twitter varchar(16) NULL,
	category varchar NULL,
	chains varchar array NULL
);
