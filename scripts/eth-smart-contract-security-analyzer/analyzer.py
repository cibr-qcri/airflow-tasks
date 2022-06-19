import os
import sys

import psycopg2
from psycopg2 import Error


def get_eth_contracts():
    d = dict()
    query = "SELECT gecko_id, symbol, name, platform->'ethereum' " \
            "FROM coingecko_protocol WHERE platform::jsonb ? 'ethereum'"
    try:
        gp_cursor.execute(query)
        results = gp_cursor.fetchall()
        for result in results:
            d[result[3]] = {'gecko_id': result[0], 'symbol': result[1], 'name': result[2], 'contract': result[3]}
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()
    return d


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
        gp_cursor.execute("SELECT version();")
        record = gp_cursor.fetchone()
        print("You are connected to - ", record, "\n")
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


if __name__ == '__main__':
    gp_connection = None
    gp_cursor = None
    connects_to_greenplum()
    get_eth_contracts()
    close_gp_connection()
