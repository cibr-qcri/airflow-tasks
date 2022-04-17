-- This function responsible for populate table with values
create or replace function enrich_tmp_btc_wallet_table()
returns void
as $$
begin 
    PERFORM wallets_for_missing_outputs();
    raise notice 'Wallet assigns for all missing output addresses!';
    PERFORM create_wallets();
    raise notice 'Wallet tabel creation is complete!';
    PERFORM enrich_wallet_total_spent();
    raise notice 'Wallet tabel is updated with total spent amounts!';
    PERFORM enrich_wallet_total_spent_usd();
    raise notice 'Wallet tabel is updated with total spent usd amounts!';
    PERFORM enrich_wallet_total_received();
    raise notice 'Wallet tabel is updated with total received amounts!';
    PERFORM enrich_wallet_total_received_usd();
    raise notice 'Wallet tabel is updated with total received usd amounts!';
    PERFORM enrich_wallet_total_tx();
    raise notice 'Wallet tabel is updated with total tx counts!';
    PERFORM enrich_wallet_labels();
    raise notice 'Wallet tabel is updated with labels!';
    PERFORM enrich_wallet_categories();
    raise notice 'Wallet tabel is updated with categories!';
end ;
$$ language plpgsql;

create or replace function wallets_for_missing_outputs()
returns void
as $$
begin 
    insert into tmp_btc_address_cluster(cluster_id, address) (
      SELECT (uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring)) as cluster_id, address from (
      SELECT distinct address FROM btc_tx_output WHERE NOT EXISTS(SELECT address FROM tmp_btc_address_cluster WHERE tmp_btc_address_cluster.address=btc_tx_output.address)) as missing_address
    );
end ;
$$ language plpgsql;

create or replace function create_wallets()
returns void
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
begin
    num_rows := (select MAX(id) FROM tmp_btc_address_cluster);
    WHILE start_num <= num_rows LOOP
    end_num := (start_num + chunk_size);
    -- query starts
    insert into tmp_btc_wallet(cluster_id, num_address) (select cluster_id, count(address) as total_address from tmp_btc_address_cluster where id >= start_num and id < end_num group by cluster_id);
    -- query ends
    raise notice 'create wallet: processed address range % - %',start_num, end_num;
    start_num := (start_num + chunk_size);
    END LOOP;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_spent()
returns void
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
begin
    num_rows := (select MAX(id) FROM btc_tx_input);
    WHILE start_num <= num_rows LOOP
    end_num := (start_num + chunk_size);
     -- query starts
    UPDATE
      tmp_btc_wallet
    SET
      total_spent=total_spent + total_spent_table.total_spent
    FROM
      (
        select * from (
            select SUM(total_value) as total_spent, cluster_id from (
            select all_inputs.address, total_value, cluster_id from (
            select address, SUM(tx_value) as total_value from btc_tx_input where id >= start_num and id < end_num group by address) AS all_inputs 
            INNER JOIN (select address, cluster_id from tmp_btc_address_cluster) as cluster 
            ON cluster.address = all_inputs.address) as total_spent 
            group by cluster_id) as final
      ) AS total_spent_table
    WHERE
      tmp_btc_wallet.cluster_id=total_spent_table.cluster_id;
    -- query ends
    raise notice 'update wallet total spent: processed input range % - %',start_num, end_num;
    start_num := (start_num + chunk_size);
    END LOOP;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_spent_usd()
returns void
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
begin
    num_rows := (select MAX(id) FROM btc_tx_input);
    WHILE start_num <= num_rows LOOP
    end_num := (start_num + chunk_size);
    -- query starts
    UPDATE
      tmp_btc_wallet
    SET
      total_spent_usd=total_spent_usd + total_spent_usd_table.total_spent_usd
    FROM
      (
        select * from (
            select SUM(total_usd_value) as total_spent_usd, cluster_id from (
            select all_inputs.address, total_usd_value, cluster_id from (
            select address, SUM(usd_value) as total_usd_value from btc_tx_input where id >= start_num and id < end_num group by address) AS all_inputs 
            INNER JOIN (select address, cluster_id from tmp_btc_address_cluster) as cluster 
            ON cluster.address = all_inputs.address) as total_spent_usd 
            group by cluster_id) as final
      ) AS total_spent_usd_table
    WHERE
      tmp_btc_wallet.cluster_id=total_spent_usd_table.cluster_id;
    -- query ends
    raise notice 'update wallet total spent USD: processed input range % - %',start_num, end_num;
    start_num := (start_num + chunk_size);
    END LOOP;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_received()
returns void
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
begin
    num_rows := (select MAX(id) FROM btc_tx_output);
    WHILE start_num <= num_rows LOOP
    end_num := (start_num + chunk_size);
    -- query starts
    UPDATE
      tmp_btc_wallet
    SET
      total_received=total_received + total_received_table.total_received
    FROM
      (
        select * from (
          select SUM(total_value) as total_received, cluster_id from (
          select all_outputs.address, total_value, cluster_id from (
          select address, SUM(tx_value) as total_value from btc_tx_output where id >= start_num and id < end_num group by address) AS all_outputs 
          INNER JOIN (select address, cluster_id from tmp_btc_address_cluster) as cluster 
          ON cluster.address = all_outputs.address) as total_received 
        group by cluster_id) as final
      ) AS total_received_table
    WHERE
      tmp_btc_wallet.cluster_id=total_received_table.cluster_id;
    -- query ends
    raise notice 'update wallet total received: processed output range % - %',start_num, end_num;
    start_num := (start_num + chunk_size);
    END LOOP;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_received_usd()
returns void
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
begin 
    num_rows := (select MAX(id) FROM btc_tx_output);
    WHILE start_num <= num_rows LOOP
    end_num := (start_num + chunk_size);
    -- query starts
    UPDATE
      tmp_btc_wallet
    SET
      total_received_usd=total_received_usd + total_received_usd_table.total_received_usd
    FROM
      (
        select * from (
          select SUM(total_usd_value) as total_received_usd, cluster_id from (
          select all_outputs.address, total_usd_value, cluster_id from (
          select address, SUM(usd_value) as total_usd_value from btc_tx_output where id >= start_num and id < end_num group by address) AS all_outputs 
          INNER JOIN (select address, cluster_id from tmp_btc_address_cluster) as cluster 
          ON cluster.address = all_outputs.address) as total_received 
        group by cluster_id) as final
      ) AS total_received_usd_table
    WHERE
      tmp_btc_wallet.cluster_id=total_received_usd_table.cluster_id;
    -- query ends
    raise notice 'update wallet total received USD: processed output range % - %',start_num, end_num;
    start_num := (start_num + chunk_size);
    END LOOP;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_tx()
returns void
as $$
begin 
    UPDATE
      tmp_btc_wallet
    SET
      num_tx=num_tx + total_tx_count_table.tx_count
    FROM
      (
        SELECT cluster_id, count(DISTINCT tx_hash) AS tx_count FROM tmp_btc_address_cluster JOIN (
          SELECT address, tx_hash FROM ( 
            SELECT address,tx_hash FROM btc_tx_input 
            UNION 
            SELECT address,tx_hash FROM btc_tx_output
            ) as addresses
          ) as txes 
        ON tmp_btc_address_cluster.address=txes.address GROUP BY cluster_id
      ) AS total_tx_count_table
    WHERE
      tmp_btc_wallet.cluster_id=total_tx_count_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_labels()
returns void
as $$
begin 
    UPDATE
      tmp_btc_wallet
    SET
      labels=label_wallets.json
    FROM
      (
      SELECT cluster_id, CAST(json_object_agg(element, count) AS TEXT) as json from (
      with elements (cluster_id, element) as (
      select cluster_id, unnest(string_to_array(labels, ',')) from (
      select tmp_btc_address_cluster.cluster_id, array_to_string(array_agg(btc_address_label.label), ',') as labels from 
      tmp_btc_address_cluster join btc_address_label ON tmp_btc_address_cluster.address=btc_address_label.address 
      group by tmp_btc_address_cluster.cluster_id
      ) as labels group by cluster_id, labels) select cluster_id, element, count(*) as count 
      from elements group by cluster_id, element order by count desc) as data group by cluster_id
      ) AS label_wallets
    WHERE tmp_btc_wallet.cluster_id=label_wallets.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_categories()
returns void
as $$
begin 
    UPDATE
      tmp_btc_wallet
    SET
      categories=category_wallets.json
    FROM
      (
      SELECT cluster_id, CAST(json_object_agg(element, count) AS TEXT) as json from (
		  with elements (cluster_id, element) as (
		  select cluster_id, unnest(string_to_array(categories, ',')) from (
		  select btc_address_cluster.cluster_id, array_to_string(array_agg(btc_address_label.category), ',') as categories from 
		  btc_address_cluster join btc_address_label ON btc_address_cluster.address=btc_address_label.address 
		  group by btc_address_cluster.cluster_id
		  ) as categories group by cluster_id, categories) select cluster_id, element, count(*) as count 
		  from elements group by cluster_id, element order by count desc) as data group by cluster_id
      ) AS category_wallets
    WHERE
      tmp_btc_wallet.cluster_id=category_wallets.cluster_id;
end ;
$$ language plpgsql;
