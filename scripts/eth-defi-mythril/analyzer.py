import json
import os
import sys

import subprocess
import psycopg2
from psycopg2 import Error


def persist_report(report):
    pdata = [report['symbol'], report['name'], report['source'], report['contract'], report['data']]

    query = 'INSERT INTO eth_defi_scan_result (symbol, name, source, contract, data) ' \
            'VALUES (%s, %s, %s, %s, %s);'

    try:
        gp_cursor.execute(query, tuple(pdata))
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()


def analyze_contracts():
    query = "SELECT symbol, name, contract " \
            "FROM eth_defi_protocol " \
            "WHERE contract NOT IN (SELECT contract FROM eth_defi_scan_result WHERE source='mythril')"
    try:
        gp_cursor.execute(query)
        results = gp_cursor.fetchall()
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)
    finally:
        gp_connection.commit()

    for result in results:
        contract = result[2]
        contract_report = analyze_contract(contract)
        report = {'symbol': result[0], 'name': result[1], 'source': 'mythril', 'contract': contract,
                  'data': contract_report}
        persist_report(report)


def analyze_contract(contract):
    # rpc = '{}:{}'.format(os.getenv('ETHEREUM_CLIENT_HOST'), os.getenv('ETHEREUM_CLIENT_PORT'))
    result = subprocess.run(['myth', 'analyze', '-a', contract, '-o', 'jsonv2', '--infura-id',
                             '5c764c63ab10434db0d49c248317fee9'], stdout=subprocess.PIPE).stdout.decode('utf-8')

    return result


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
    analyze_contracts()
    close_gp_connection()
