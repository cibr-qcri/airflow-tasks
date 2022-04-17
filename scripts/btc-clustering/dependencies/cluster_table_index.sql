do $$
begin
    if exists(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_address_cluster') then
      ALTER TABLE btc_address_cluster RENAME TO btc_address_cluster_old;
      ALTER TABLE tmp_btc_address_cluster RENAME TO btc_address_cluster;
      drop table btc_address_cluster_old;
    end if;

    if exists(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_wallet') then
      ALTER TABLE btc_wallet RENAME TO btc_wallet_old;
      ALTER TABLE tmp_btc_wallet RENAME TO btc_wallet;
      drop table btc_wallet_old;
    end if;

    if exists(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_wallet_transaction') then
      ALTER TABLE btc_wallet_transaction RENAME TO btc_wallet_transaction_old;
      ALTER TABLE tmp_btc_wallet_transaction RENAME TO btc_wallet_transaction;
      drop table btc_wallet_transaction_old;
    end if;
end; $$ language plpgsql;

-- btc_wallet indexes
CREATE INDEX IF NOT EXISTS wallet_group_comp_index ON btc_wallet(cluster_id, num_address, num_tx, total_spent_usd, total_received_usd, risk_score, id);
CREATE INDEX IF NOT EXISTS  wallet_index ON btc_wallet(cluster_id);
CREATE INDEX IF NOT EXISTS  wallet_comp_index ON btc_wallet(id, cluster_id);

-- btc_address_cluster indexes
CREATE INDEX IF NOT EXISTS  btc_wallet_address_comp_index ON btc_address_cluster(id, cluster_id);
CREATE INDEX IF NOT EXISTS  btc_wallet_address_index ON btc_address_cluster(address);
CREATE INDEX IF NOT EXISTS  btc_wallet_id_index ON btc_address_cluster(cluster_id);

-- btc_wallet_transaction indexes
CREATE INDEX IF NOT EXISTS  btc_wallet_transaction_comp_index ON btc_wallet_transaction(id, cluster_id);
