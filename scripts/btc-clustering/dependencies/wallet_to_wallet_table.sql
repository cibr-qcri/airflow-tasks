CREATE TABLE tmp_btc_link_wallet (
	id SERIAL primary key NOT NULL,
	first_cluster varchar(65),
    second_cluster varchar(65),
	num_txes_to_second integer DEFAULT 0,
    num_txes_from_second integer DEFAULT 0,
    first_cluster_size integer DEFAULT 0,
    second_cluster_size integer DEFAULT 0,
	total_satoshi_to_second bigint DEFAULT 0,
	total_usd_to_second numeric DEFAULT 0,
	total_satoshi_from_second bigint DEFAULT 0,
	total_usd_from_second numeric DEFAULT 0
);

CREATE TABLE tmp_unmerged_link_wallet (
	id SERIAL primary key NOT NULL,
	wallet_in varchar(65),
    wallet_out varchar(65),
	tx_hash varchar(65),
	in_satoshi_amount bigint DEFAULT 0,
	out_satoshi_amount numeric DEFAULT 0,
	in_usd_amount bigint DEFAULT 0,
	out_usd_amount numeric DEFAULT 0
);

create
or replace function create_tmp_link_wallet_table() 
returns void 
as $$
DECLARE start_num int :=1;
DECLARE chunk_size int :=5000000;
DECLARE num_rows int;
DECLARE end_num int;
DECLARE total int;
begin
  num_rows := (select MAX(id) FROM tmp_btc_address_cluster);
  WHILE start_num <= num_rows LOOP
  end_num := (start_num + chunk_size);
  -- query starts
  insert into tmp_unmerged_link_wallet
  (wallet_in, wallet_out, tx_hash, in_satoshi_amount, out_satoshi_amount, in_usd_amount, out_usd_amount)
  (
    select wallet_in, wallet_out, in_wallets.tx_hash as tx_hash, in_satoshi_amount, out_satoshi_amount, in_usd_amount, out_usd_amount from (
      SELECT cluster_id as wallet_out, btc_input_addresses.address as address, tx_hash, tx_value as out_satoshi_amount, usd_value as out_usd_amount 
      from (select address, cluster_id from btc_address_cluster where id >= start_num and id < end_num order by id asc) as btc_input_addresses 
      INNER JOIN btc_tx_input on btc_input_addresses.address=btc_tx_input.address
      ) as out_wallets
    inner join (
      SELECT cluster_id as wallet_in, btc_output_addresses.address as address, tx_hash, tx_value as in_satoshi_amount, usd_value as in_usd_amount 
      from (select address, cluster_id from btc_address_cluster where id >= start_num and id < end_num order by id asc) as btc_output_addresses 
      INNER JOIN btc_tx_output on btc_output_addresses.address=btc_tx_output.address 
      ) as in_wallets 
    on in_wallets.tx_hash=out_wallets.tx_hash;
  );
   -- query ends
  raise notice 'create-tmp_unmerged_link_wallet: processed address range % - %',start_num, end_num;
  start_num := (start_num + chunk_size);
  END LOOP;

  raise notice 'tmp_unmerged_link_wallet table filled!';
end;
$$ language plpgsql;

