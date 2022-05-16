-- This function responsible for populate table with values
create or replace function enrich_btc_wallet_transaction_table()
returns void
as $$
begin 
    PERFORM create_wallet_transactions();
    raise notice 'Wallet transaction table is enriched!';
end ;
$$ language plpgsql;

create
or replace function create_wallet_transactions() 
returns void 
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=10000000;
DECLARE num_rows int;
DECLARE end_num int;
DECLARE total int;
begin
  num_rows := (select MAX(id) FROM tmp_btc_address_cluster);
  WHILE start_num <= num_rows LOOP
  end_num := (start_num + chunk_size);
  -- query starts
  insert into
    tmp_btc_wallet_transaction(
      cluster_id,
      tx_hash,
      block_number,
      input_value,
      output_value,
      is_coinbase,
      input_count,
      output_count,
      tx_type,
      timestamp
    ) (
      select
        cluster_id,
        wallet_txes.tx_hash,
        block_number,
        input_value,
        output_value,
        is_coinbase,
        input_count,
        output_count,
        'Sending' as tx_type,
        timestamp
      from
        (
          select
            distinct tx_hash,
            cluster_id
          from
            (select address, cluster_id from tmp_btc_address_cluster where id >= start_num and id < end_num) as temp_btc_address_cluster
            inner join btc_tx_input on temp_btc_address_cluster.address = btc_tx_input.address
          group by
            cluster_id,
            tx_hash
        ) as wallet_txes
        inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
  );
  -- query ends
  raise notice 'create-btc-transaction for inputs: processed address range % - %',start_num, end_num;
  start_num := (start_num + chunk_size);
  END LOOP;

  raise notice 'tmp_btc_wallet_transaction table filled with spent data!';

  start_num := 1;
  WHILE start_num <= num_rows LOOP
  end_num := (start_num + chunk_size);
  insert into
    tmp_btc_wallet_transaction(
      cluster_id,
      tx_hash,
      block_number,
      input_value,
      output_value,
      is_coinbase,
      input_count,
      output_count,
      tx_type,
      timestamp
    ) (
      select
        cluster_id,
        wallet_txes.tx_hash,
        block_number,
        input_value,
        output_value,
        is_coinbase,
        input_count,
        output_count,
        'Receiving' as tx_type,
        timestamp
      from
        (
          select
            distinct tx_hash,
            cluster_id
          from
            (select address, cluster_id from tmp_btc_address_cluster where id >= start_num and id < end_num) as temp_btc_address_cluster
            inner join btc_tx_output on temp_btc_address_cluster.address = btc_tx_output.address
          group by
            cluster_id,
            tx_hash
        ) as wallet_txes
        inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
    );
  -- query ends
  raise notice 'create-btc-transaction for outputs: processed address range % - %',start_num, end_num;
  start_num := (start_num + chunk_size);
  END LOOP;

  raise notice 'tmp_btc_wallet_transaction table filled with received data!';
end;
$$ language plpgsql;
