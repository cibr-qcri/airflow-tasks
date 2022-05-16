do $$
begin
    if EXISTS(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_address_cluster') then
      ALTER TABLE IF EXISTS btc_address_cluster RENAME TO btc_address_cluster_old;
      ALTER TABLE IF EXISTS tmp_btc_address_cluster RENAME TO btc_address_cluster;
      DROP TABLE IF EXISTS btc_address_cluster_old;
    end if;

    if EXISTS(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_wallet') then
      ALTER TABLE IF EXISTS btc_wallet RENAME TO btc_wallet_old;
      ALTER TABLE IF EXISTS tmp_btc_wallet RENAME TO btc_wallet;
      DROP TABLE IF EXISTS btc_wallet_old;
    end if;

    if EXISTS(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_wallet_transaction') then
      ALTER TABLE IF EXISTS btc_wallet_transaction RENAME TO btc_wallet_transaction_old;
      ALTER TABLE IF EXISTS tmp_btc_wallet_transaction RENAME TO btc_wallet_transaction;
      DROP TABLE IF EXISTS btc_wallet_transaction_old;
    end if;

    -- if EXISTS(select 1 from information_schema.tables where table_schema = current_schema() and table_name = 'tmp_btc_link_wallet') then
    --   ALTER TABLE IF EXISTS btc_link_wallet RENAME TO btc_link_wallet_old;
    --   ALTER TABLE IF EXISTS tmp_btc_link_wallet RENAME TO btc_link_wallet;
    --   DROP TABLE IF EXISTS btc_link_wallet_old;
    -- end if;
end; $$ language plpgsql;

-- btc_wallet indexes
DROP INDEX wallet_group_comp_index;
CREATE INDEX wallet_group_comp_index ON btc_wallet(cluster_id, num_address, num_tx, total_spent_usd, total_received_usd, risk_score, id);
DROP INDEX wallet_index;
CREATE INDEX wallet_index ON btc_wallet(cluster_id);
DROP INDEX wallet_comp_index;
CREATE INDEX wallet_comp_index ON btc_wallet(id, cluster_id);

-- btc_address_cluster indexes
DROP INDEX btc_wallet_address_comp_index;
CREATE INDEX btc_wallet_address_comp_index ON btc_address_cluster(id, cluster_id);
DROP INDEX btc_wallet_address_index;
CREATE INDEX btc_wallet_address_index ON btc_address_cluster(address);
DROP INDEX btc_wallet_id_index;
CREATE INDEX btc_wallet_id_index ON btc_address_cluster(cluster_id);

-- btc_wallet_transaction indexes
DROP INDEX btc_wallet_transaction_comp_index;
CREATE INDEX btc_wallet_transaction_comp_index ON btc_wallet_transaction(id, cluster_id);

-- btc_link_wallet indexes
-- CREATE INDEX IF NOT EXISTS  btc_link_wallet_index ON btc_link_wallet(first_cluster);
-- CREATE INDEX IF NOT EXISTS  btc_link_wallet_comp_index ON btc_link_wallet(id, first_cluster);
