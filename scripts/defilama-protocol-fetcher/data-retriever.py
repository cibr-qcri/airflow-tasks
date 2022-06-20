import os
import sys

import psycopg2
from messari.defillama import DeFiLlama
from psycopg2 import Error
from sqlalchemy import create_engine


def fetch_protocols():
    dl = DeFiLlama()
    d = dl.get_protocols().T
    d.rename(columns={'cmcId': 'cmc_id'}, inplace=True)

    return d[data]


def connects_to_greenplum():
    try:
        global gp_connection
        global gp_cursor
        gp_connection = psycopg2.connect(user=os.getenv('GREENPLUM_USERNAME'),
                                         password=os.getenv('GREENPLUM_PASSWORD'),
                                         host=os.getenv('GREENPLUM_HOST'),
                                         port=os.getenv('GREENPLUM_SERVICE_PORT'),
                                         database=os.getenv('GREENPLUM_DB'))
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute((open('defi_protocol_schema.sql', 'r').read()))
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


def persist():
    db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}' \
        .format(os.getenv('GREENPLUM_USERNAME'), os.getenv('GREENPLUM_PASSWORD'),
                os.getenv('GREENPLUM_HOST'), os.getenv('GREENPLUM_SERVICE_PORT'), os.getenv('GREENPLUM_DB'))

    engine = create_engine(db_url)
    protocols.to_sql('temporary_table', engine, if_exists='append', method='multi', index=False)
    with engine.begin() as cnx:
        insert_sql = 'INSERT INTO eth_defi_data_provider (id, name, address, symbol, url, description, chain, ' \
                     'gecko_id, cmc_id, twitter, category, chains) ' \
                     'SELECT id, name, address, symbol, url, description, chain, gecko_id, cmc_id, twitter, ' \
                     'category, chains ' \
                     'FROM temporary_table ' \
                     'WHERE id NOT IN (SELECT DISTINCT id FROM eth_defi_data_provider)'
        cnx.execute(insert_sql)


if __name__ == '__main__':
    gp_connection = None
    gp_cursor = None
    connects_to_greenplum()
    data = ['id', 'name', 'address', 'symbol', 'url', 'description', 'chain', 'gecko_id',
            'cmc_id', 'twitter', 'category', 'chains']
    protocols = fetch_protocols()
    persist()
    close_gp_connection()
