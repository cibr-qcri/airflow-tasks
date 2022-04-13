-- This function responsible for populate table with values
create or replace function enrich_btc_wallet_transaction_table()
returns void
as $$
begin 
    PERFORM enrich_btc_wallet_transaction();
    raise notice 'Wallet transaction table is enriched!';
end ;
$$ language plpgsql;

create
or replace function create_wallet_transactions() 
returns void 
as $$ 
begin

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
      tx_type
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
        'Sending' as tx_type
      from
        (
          select
            distinct tx_hash,
            cluster_id
          from
            tmp_btc_address_cluster
            inner join btc_tx_input on tmp_btc_address_cluster.address = btc_tx_input.address
          group by
            cluster_id,
            tx_hash
        ) as wallet_txes
        inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
    );

  raise notice 'tmp_btc_wallet_transaction table filled with spent data!';

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
      tx_type
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
        'Receiving' as tx_type
      from
        (
          select
            distinct tx_hash,
            cluster_id
          from
            tmp_btc_address_cluster
            inner join btc_tx_output on tmp_btc_address_cluster.address = btc_tx_output.address
          group by
            cluster_id,
            tx_hash
        ) as wallet_txes
        inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
    );

  raise notice 'tmp_btc_wallet_transaction table filled with received data!';

end;
$$ language plpgsql;
