CREATE TABLE IF NOT EXISTS coingecko_protocol (
    gecko_id VARCHAR PRIMARY KEY NOT NULL,
    symbol VARCHAR,
    name VARCHAR,
    asset_platform_id VARCHAR,
    genesis_date DATE NULL,
    platform JSONB,
    category JSONB,
    description TEXT,
    contract_address VARCHAR
);

/*
CREATE TABLE IF NOT EXISTS protocol_market_data (
    protocol VARCHAR PRIMARY KEY NOT NULL,
    current_price FLOAT,
    total_value_locked JSONB,
    mcap_to_tvl_ratio FLOAT,
    fdv_to_tvl_ratio FLOAT,
    ath FLOAT,
    ath_change_percentage FLOAT,
    ath_date DATE,
    atl FLOAT,
    atl_change_percentage FLOAT,
    atl_date DATE ,
    marketcap FLOAT,
    fully_diluted_valuation FLOAT,
    total_volume FLOAT,
    total_supply BIGINT,
    max_supply BIGINT,
    circulating_supply BIGINT,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS protocol_metric (
    protocol VARCHAR PRIMARY KEY NOT NULL,
    sentiment_votes_up_percentage FLOAT,
    sentiment_votes_down_percentage FLOAT,
    marketcap_rank INT,
    coingecko_rank INT,
    coingecko_score FLOAT,
    developer_score FLOAT,
    community_score FLOAT,
    liquidity_score FLOAT,
    public_interest_score FLOAT,
    community_data JSONB,
    developer_data JSON,
    public_interest_stat JSONB
);

CREATE TABLE IF NOT EXISTS protocol_ticker_data (
    protocol VARCHAR NOT NULL,
    base VARCHAR,
    target VARCHAR,
    market JSONB,
    last VARCHAR,
    volume VARCHAR,
    converted_last JSONB,
    converted_volume JSONB,
    trust_score VARCHAR,
    bid_ask_spread_percentage VARCHAR,
    last_traded_at VARCHAR,
    is_anomaly VARCHAR,
    is_stale VARCHAR
);
*/