CREATE TABLE IF NOT EXISTS defilama_protocol (
	symbol varchar primary key NOT NULL,
	created_at timestamp default current_timestamp,
	name varchar,
	address varchar,
	url varchar,
	description varchar,
	chain varchar,
	gecko_id varchar,
	cmcId integer,
	twitter varchar(16),
	category varchar,
	chains varchar []
);
