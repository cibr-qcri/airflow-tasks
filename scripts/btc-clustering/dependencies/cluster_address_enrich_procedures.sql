-- This function responsible for populate table with values
create or replace function enrich_tmp_btc_address_cluster_table()
returns void
as $$
begin 
    PERFORM enrich_wallet_address_spent_amounts();
    raise notice 'Address cluster tabel is updated with total spent amounts!';
    PERFORM enrich_wallet_address_received_amounts();
    raise notice 'Address cluster is updated with total spent usd amounts!';
    PERFORM enrich_address_labels_with_cluster_id();
    raise notice 'Address cluster is updated with labels!';
end ;
$$ language plpgsql;

create or replace function enrich_wallet_address_spent_amounts() 
returns void 
as $$ 
begin 
    DROP TABLE IF EXISTS btc_wallet_address_spent;
    CREATE TABLE btc_wallet_address_spent (
    id integer primary key NOT NULL,
    total_spent_satoshi bigint DEFAULT 0,
    total_spent_usd numeric DEFAULT 0
    );

    raise notice 'Temporary btc_wallet_address_spent table created!';
    insert into
    btc_wallet_address_spent(id, total_spent_satoshi, total_spent_usd) (
        select
        tmp_btc_address_cluster.id,
        sum(tx_value) as total_spent_satoshi,
        sum(usd_value) as total_spent_usd
        from
        tmp_btc_address_cluster
        INNER JOIN btc_tx_input ON tmp_btc_address_cluster.address = btc_tx_input.address
        group by
        tmp_btc_address_cluster.id
    );
    raise notice 'btc_wallet_address_spent table filled with data!';

    UPDATE
    tmp_btc_address_cluster
    SET
    total_spent_satoshi = btc_wallet_address_spent.total_spent_satoshi,
    total_spent_usd = btc_wallet_address_spent.total_spent_usd
    FROM
    btc_wallet_address_spent
    WHERE
    tmp_btc_address_cluster.id = btc_wallet_address_spent.id;

    raise notice 'tmp_btc_address_cluster is updated with btc_wallet_address_spent data!';
    DROP TABLE IF EXISTS btc_wallet_address_spent;
end;
$$ language plpgsql;

create or replace function enrich_wallet_address_received_amounts() 
returns void 
as $$ 
begin
    DROP TABLE IF EXISTS btc_wallet_address_received;
    CREATE TABLE btc_wallet_address_received (
        id integer primary key,
        total_received_satoshi bigint DEFAULT 0,
        total_received_usd numeric DEFAULT 0
    );
    raise notice 'Temporary btc_wallet_address_received table created!';

    insert into
    btc_wallet_address_received(id, total_received_satoshi, total_received_usd) (
        select
        tmp_btc_address_cluster.id,
        sum(tx_value) as total_received_satoshi,
        sum(usd_value) as total_received_usd
        from
        tmp_btc_address_cluster
        INNER JOIN btc_tx_output ON tmp_btc_address_cluster.address = btc_tx_output.address
        group by
        tmp_btc_address_cluster.id
    );
    raise notice 'btc_wallet_address_received table filled with data!';

    UPDATE
    tmp_btc_address_cluster
    SET
    total_received_satoshi = btc_wallet_address_received.total_received_satoshi,
    total_received_usd = btc_wallet_address_received.total_received_usd
    FROM
    btc_wallet_address_received
    WHERE
    tmp_btc_address_cluster.id = btc_wallet_address_received.id;

    raise notice 'tmp_btc_address_cluster is updated with btc_wallet_address_received data!';
    DROP TABLE IF EXISTS btc_wallet_address_received;

end;
$$ language plpgsql;

create or replace function enrich_address_labels_with_cluster_id()
returns void
as $$
begin 
    UPDATE
      btc_address_label
    SET
      cluster_id=tmp_btc_address_cluster.cluster_id
    FROM
      tmp_btc_address_cluster
    WHERE
      btc_address_label.address=tmp_btc_address_cluster.address;
end ;
$$ language plpgsql;
