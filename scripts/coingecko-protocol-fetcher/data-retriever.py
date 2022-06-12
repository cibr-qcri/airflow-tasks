import json
import os
import sys
import time
from types import SimpleNamespace

import psycopg2
from psycopg2 import Error
from pycoingecko import CoinGeckoAPI


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
        gp_cursor.execute((open('defi_protocol_schema.sql', 'r').read()))
        gp_connection.commit()
    except (Exception, Error) as error:
        sys.exit("Error while connecting to PostgreSQL", error)


def close_gp_connection():
    try:
        if gp_connection:
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)


def fetch_coin_ids():
    coin_ids = []
    try:
        query = (f"SELECT gecko_id from defilama_protocol "
                 f"WHERE gecko_id IS NOT NULL "
                 f"AND gecko_id NOT IN (SELECT gecko_id FROM coingecko_protocol);")
        gp_cursor.execute(query)
        coin_ids = [x[0] for x in gp_cursor.fetchall()]
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()

    return coin_ids


def persist_protocol(coin):
    description = SimpleNamespace(**coin.description).en if coin.description else ''
    categories = [x for x in coin.categories if x is not None]
    genesis_date = coin.genesis_date if coin.genesis_date else None
    contract_address = coin.contract_address if hasattr(coin, 'contract_address') else None
    pdata = [coin.id, coin.symbol, coin.name, coin.asset_platform_id, genesis_date, json.dumps(coin.platforms),
             json.dumps(list(categories)), description, contract_address]

    query = 'INSERT INTO coingecko_protocol (gecko_id, symbol, name, asset_platform_id, genesis_date, platform, ' \
            'category, description, contract_address) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'

    try:
        gp_cursor.execute(query, tuple(pdata))
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()


def fetch_coins(coin_ids):
    cg = CoinGeckoAPI()
    for coin_id in coin_ids:
        try:
            coin = cg.get_coin_by_id(coin_id)
            coin = SimpleNamespace(**coin)
            persist_protocol(coin)
        except ValueError:
            print('Could not find coin with the given id: :' + coin_id)
        time.sleep(1.2)


if __name__ == '__main__':
    gp_connection = None
    gp_cursor = None
    connects_to_greenplum()
    fetch_coins(fetch_coin_ids())
    close_gp_connection()
