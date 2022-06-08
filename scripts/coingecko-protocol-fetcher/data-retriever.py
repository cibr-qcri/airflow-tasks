import json
import os
import sys
from types import SimpleNamespace

import psycopg2
from psycopg2 import Error
from pycoingecko import CoinGeckoAPI


def execute_query(query):
    try:
        gp_cursor.execute(query)
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()


def persist_metrics_data(coin):
    fields = "protocol, sentiment_votes_up_percentage, sentiment_votes_down_percentage, marketcap_rank, " \
             "coingecko_rank, coingecko_score, developer_score, community_score, liquidity_score, " \
             "public_interest_score, community_data, developer_data, public_interest_stat"

    mdata = [coin.id, coin.sentiment_votes_up_percentage, coin.sentiment_votes_down_percentage, coin.market_cap_rank,
             coin.coingecko_rank, coin.coingecko_score, coin.developer_score, coin.community_score,
             coin.liquidity_score, coin.public_interest_score, json.dumps(coin.community_data),
             json.dumps(coin.developer_data), json.dumps(coin.public_interest_stats)]

    query = (f"INSERT INTO protocol_metric ({fields}) "
             f"VALUES{tuple(mdata)};")
    # print(query)
    execute_query(query)


def persist_ticker_data(coin):
    fields = "protocol, base, target, market, last, volume, converted_last, converted_volume, trust_score, " \
             "bid_ask_spread_percentage, last_traded_at, is_anomaly, is_stale"
    targets = ['USD', 'USDT', 'USDC', 'BUSD']
    tickers = coin.tickers
    for ticker in tickers:
        ticker = SimpleNamespace(**ticker)
        if ticker.target in targets:
            tdata = [coin.id, ticker.base, ticker.target, json.dumps(ticker.market), ticker.last, ticker.volume,
                     json.dumps(ticker.converted_last), json.dumps(ticker.converted_volume), ticker.trust_score,
                     ticker.bid_ask_spread_percentage,
                     ticker.last_traded_at, ticker.is_anomaly, ticker.is_stale]
            query = (f"INSERT INTO protocol_ticker_data ({fields}) "
                     f"VALUES{tuple(tdata)};")
            execute_query(query)


def persist_market_data(coin):
    md = SimpleNamespace(**coin.market_data)
    fields = "protocol, current_price, total_value_locked, mcap_to_tvl_ratio, fdv_to_tvl_ratio, ath, " \
             "ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date, marketcap, " \
             "fully_diluted_valuation, total_volume, total_supply, max_supply, circulating_supply, last_updated"

    mdata = [coin.id, md.current_price['usd'], json.dumps(md.total_value_locked), md.mcap_to_tvl_ratio,
             float(md.fdv_to_tvl_ratio), md.ath['usd'], md.ath_change_percentage['usd'], md.ath_date['usd'],
             md.atl['usd'],
             md.atl_change_percentage['usd'], md.atl_date['usd'], md.market_cap['usd'],
             md.fully_diluted_valuation['usd'],
             md.total_volume['usd'], md.total_supply, md.max_supply, md.circulating_supply, md.last_updated]

    query = (f"INSERT INTO protocol_market_data ({fields}) "
             f"VALUES{tuple(mdata)};")
    execute_query(query)


def persist_protocol(coin):
    fields = "gecko_id, symbol, name, asset_platform_id, genesis_date, platform, category, description, " \
             "contract_address"

    description = SimpleNamespace(**coin.description).en if coin.description else ''
    categories = [x for x in coin.categories if x is not None]
    pdata = [coin.id, coin.symbol, coin.name, coin.asset_platform_id, coin.genesis_date, json.dumps(coin.platforms),
             json.dumps(list(categories)), description, coin.contract_address]

    query = (f"INSERT INTO protocol ({fields}) "
             f"VALUES{tuple(pdata)};")
    execute_query(query)


def fetch_coin():
    coin = cg.get_coin_by_id('maker')
    coin = SimpleNamespace(**coin)
    persist_protocol(coin)
    persist_market_data(coin)
    persist_metrics_data(coin)
    persist_ticker_data(coin)


def connects_to_greenplum():
    try:
        global gp_connection
        global gp_cursor
        gp_connection = psycopg2.connect(user=os.getenv('GREENPLUM_USERNAME'),
                                         password=os.getenv('GREENPLUM_PASSWORD'),
                                         host=os.getenv('GREENPLUM_HOST'),
                                         port=os.getenv('GREENPLUM_SERVICE_PORT'),
                                         database=os.getenv('GREENPLUM_DEFI_DB'))
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute((open('dependencies/defi_protocol_schema.sql', 'r').read()))
        gp_connection.commit()
    except (Exception, Error) as error:
        sys.exit("Error while connecting to PostgreSQL", error)


def close_gp_connection():
    try:
        if gp_connection:
            sql = '''DROP TABLE temporary_table'''
            gp_cursor.execute(sql)
            gp_connection.commit()
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)


if __name__ == '__main__':
    gp_connection = None
    gp_cursor = None
    cg = CoinGeckoAPI()
    connects_to_greenplum()
    fetch_coin()
    close_gp_connection()
