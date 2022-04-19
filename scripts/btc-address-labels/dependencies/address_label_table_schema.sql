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

create or replace function remove_btc_address_label_indexes()
returns void
as $$
begin
	DROP INDEX IF EXISTS btc_address_label_comp_index;
	DROP INDEX IF EXISTS btc_address_label_wallets_index;
end ;
$$ language plpgsql;

create or replace function enrich_btc_address_label()
returns void
as $$
begin
	ALTER TABLE btc_address_label DROP COLUMN cluster_id;
	ALTER TABLE btc_address_label ADD COLUMN cluster_id varchar(65);
    UPDATE
      btc_address_label
    SET
      cluster_id=btc_address_cluster.cluster_id
    FROM
      btc_address_cluster
    WHERE btc_address_cluster.address=btc_address_label.address;

	CREATE INDEX btc_address_label_comp_index on btc_address_label(id, cluster_id);
	CREATE INDEX btc_address_label_wallets_index on btc_address_label(cluster_id);
end ;
$$ language plpgsql;

