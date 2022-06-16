import json
import os
import sys
from urllib.request import HTTPBasicAuthHandler
from arango import ArangoClient
import requests
from urllib3.connectionpool import xrange
import gc
import psycopg2
from psycopg2 import Error

WALLET_COLLECTION = 'btc_wallets/{0}'
MAX_LIST_LIMIT = 100000
gp_connection = None
gp_cursor = None
arango_connection = None
MAX_CURSOR_LIMIT = 500000

wallet_buffer = list()
edge_buffer = list()

GP_HOST=os.getenv('GREENPLUM_HOST')
GP_USERNAME=os.getenv('GREENPLUM_USERNAME')
GP_PASSWORD=os.getenv('GREENPLUM_PASSWORD')
GP_PORT=os.getenv('GREENPLUM_PORT')
GP_DB=os.getenv('GREENPLUM_DB')
ARANGO_HOST=os.getenv('ARANGO_HOST')
ARANGO_PORT=os.getenv('ARANGO_PORT')
ARANGO_USERNAME=os.getenv('ARANGO_USERNAME')
ARANGO_PASSWORD=os.getenv('ARANGO_PASSWORD')
ARANGO_URL='http://'+ARANGO_HOST+':'+ARANGO_PORT


def connects_to_greenplum():
    try:
        # Connect to an existing database
        global gp_connection
        gp_connection = psycopg2.connect(user=GP_USERNAME,
                                    password=GP_PASSWORD,
                                    host=GP_HOST,
                                    port=GP_PORT,
                                    database=GP_DB)

        # Create a cursor to perform database operations
        global gp_cursor
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute("SELECT version();")
        record = gp_cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return

    except (Exception, Error) as error:
        sys.exit("Error while connecting to PostgreSQL", error)


def connects_to_arango():
    try:
        # Connect to an existing database
        global arango_connection
        client = ArangoClient(hosts=ARANGO_URL)
        arango_connection = client.db(GP_DB, username=ARANGO_USERNAME, password=ARANGO_PASSWORD)

        print("You are connected to - ArangoDB\n")
        return

    except (Exception, Error) as error:
        sys.exit("Error while connecting to ArangoDB", error)


def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()


def execute_sql_batch_query(query):
    gp_cursor.execute(query)
    return gp_cursor


def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        sys.exit("Error while closing the connection to PostgreSQL", error)


def close_arango_connection():
    try:
        if (arango_connection):
            arango_connection.close()
            print("ArangoDB connection is closed")
    except (Exception, Error) as error:
        sys.exit("Error while closing the connection to ArangoDB", error)


def create_edge(wallet_in, wallet_out, tx_hash, in_satoshi_amount, out_satoshi_amount, in_usd_amount, out_usd_amount):
    edge = dict()
    edge['_from'] = WALLET_COLLECTION.format(wallet_out)
    edge['_to'] = WALLET_COLLECTION.format(wallet_in)
    edge['tx_hash'] = tx_hash
    edge['in_satoshi_amount'] = int(in_satoshi_amount)
    edge['out_satoshi_amount'] = int(out_satoshi_amount)
    edge['in_usd_amount'] = float(in_usd_amount)
    edge['out_usd_amount'] = float(out_usd_amount)
    edge_buffer.append(edge)


def write_wallet_vertex():
    if arango_connection.has_graph('btc_wallet_link_cluster'):
        arango_connection.graph('btc_wallet_link_cluster')
    else:
        arango_connection.create_graph('btc_wallet_link_cluster')

    wc = arango_connection.graph('btc_wallet_link_cluster')
    if wc.has_vertex_collection("btc_wallets_tmp"):
        wallets = wc.vertex_collection("btc_wallets_tmp")
    else:
        wallets = wc.create_vertex_collection("btc_wallets_tmp")

    print("[ArangoDB] start adding wallets - ", len(wallet_buffer))
    try:
        chunks = split_list_as_chunks(wallet_buffer)
        print("Available chunks to be processed - ", len(chunks))
        wallet_buffer.clear()
        count = 0
        for chunk in chunks:
            wallets.insert_many(chunk)
            count = count + len(chunk)
            print("Processed another 100000 wallets - vertices, total: ", count)
                
        chunks.clear()    
    except:
        pass

    gc.collect()


def write_wallet_edges():
    if arango_connection.has_graph('btc_wallet_link_cluster'):
        arango_connection.graph('btc_wallet_link_cluster')
    else:
        arango_connection.create_graph('btc_wallet_link_cluster')

    wc = arango_connection.graph('btc_wallet_link_cluster')

    if not wc.has_edge_definition("btc_wallet_links_tmp"):
        edges = wc.create_edge_definition(
            edge_collection="btc_wallet_links_tmp",
            from_vertex_collections=["btc_wallets_tmp", "btc_addresses"],
            to_vertex_collections=["btc_wallets_tmp", "btc_addresses"])
    else:
        edges = wc.edge_collection("btc_wallet_links_tmp")


    print("[ArangoDB] start adding wallet edges - ", len(edge_buffer))
    try:
        chunks = split_list_as_chunks(edge_buffer)
        print("Chunk count to be processed - ", len(chunks))
        edge_buffer.clear()
        for chunk in chunks:
            edges.insert_many(chunk)
        chunks.clear()
    except:
        pass

    gc.collect()


def split_list_as_chunks(data_list):
    return [data_list[x:x + MAX_LIST_LIMIT] for x in xrange(0, len(data_list), MAX_LIST_LIMIT)]


def wallet_to_graph(wallet_id):
    vertex = dict()
    vertex['_key'] = wallet_id
    wallet_buffer.append(vertex)


def arango_post_actions():
    url = '{}/_db/{}/_api/collection/{}/rename'.format(ARANGO_URL, GP_DB, 'btc_wallets')
    params = {"name": "btc_wallets_old"}
    response = requests.put(url, data=json.dumps(params), auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error renaming older collection: btc_wallets")

    url = '{}/_db/{}/_api/collection/{}/rename'.format(ARANGO_URL, GP_DB, 'btc_wallet_links')
    params = {"name": "btc_wallet_links_old"}
    response = requests.put(url, data=json.dumps(params), auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error renaming older collection: btc_wallet_link")

    url = '{}/_db/{}/_api/collection/{}/rename'.format(ARANGO_URL, GP_DB, 'btc_wallets_tmp')
    params = {"name": "btc_wallets"}
    response = requests.put(url, data=json.dumps(params), auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error renaming older collection: btc_wallets_tmp")

    url = '{}/_db/{}/_api/collection/{}/rename'.format(ARANGO_URL, GP_DB, 'btc_wallet_links_tmp')
    params = {"name": "btc_wallet_links"}
    response = requests.put(url, data=json.dumps(params), auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error renaming older collection: btc_wallet_links_tmp")

    url = '{}/_db/{}/_api/collection/{}'.format(ARANGO_URL, GP_DB, 'btc_wallets_old')
    response = requests.delete(url, auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error deleting older collection: btc_wallets_old")

    url = '{}/_db/{}/_api/collection/{}'.format(ARANGO_URL, GP_DB, 'btc_wallet_links_old')
    response = requests.delete(url, auth=HTTPBasicAuthHandler(ARANGO_USERNAME, ARANGO_PASSWORD))
    if response.status_code != 200:
        sys.exit("Error deleting older collection: btc_wallet_links_old")


def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    if not arango_connection:
        connects_to_arango()
    
    # insert wallet vertices
    btc_wallet_records = execute_sql_query("SELECT cluster_id from tmp_btc_wallet;")
    print("Total wallet count - {}".format(len(btc_wallet_records)))
    for input_row in btc_wallet_records:
        wallet_id = input_row[0]
        wallet_to_graph(wallet_id)
    write_wallet_vertex()
    
    total_wallets = execute_sql_query("SELECT max(id) from tmp_btc_wallet;")
    print("Total wallets: ", total_wallets[0][0])
    start_index = 0
    end_index = int(total_wallets[0][0])
    chunk_size = 1000000
    
    while start_index <= end_index:
        # check in input addresses
        cursor = gp_connection.cursor(name='fetch_large_result')
        print("Query wallet map for input address range {} - {}".format(start_index, start_index+chunk_size))
        cursor.execute("""
                    select wallet_in, wallet_out, in_wallets.tx_hash as tx_hash, SUM(in_satoshi_amount) as in_satoshi_amount, SUM(out_satoshi_amount) as out_satoshi_amount, SUM(in_usd_amount) as in_usd_amount, SUM(out_usd_amount) as out_usd_amount from (
                    SELECT btc_tx_input.id as id1, cluster_id as wallet_out, btc_input_addresses.address as address, tx_hash, tx_value as out_satoshi_amount, usd_value as out_usd_amount 
                    from (select address, tmp_btc_address_cluster.cluster_id from tmp_btc_address_cluster JOIN tmp_btc_wallet ON tmp_btc_wallet.cluster_id=tmp_btc_address_cluster.cluster_id where tmp_btc_wallet.id >= {} and tmp_btc_wallet.id <= {}) as btc_input_addresses 
                    INNER JOIN btc_tx_input on btc_input_addresses.address=btc_tx_input.address
                    ) as out_wallets
                    inner join (
                    SELECT btc_tx_output.id as id2, cluster_id as wallet_in, btc_output_addresses.address as address, tx_hash, tx_value as in_satoshi_amount, usd_value as in_usd_amount 
                    from (select address, cluster_id from tmp_btc_address_cluster) as btc_output_addresses 
                    INNER JOIN btc_tx_output on btc_output_addresses.address=btc_tx_output.address 
                    ) as in_wallets 
                    on in_wallets.tx_hash=out_wallets.tx_hash group by wallet_in, wallet_out, in_wallets.tx_hash;
                    """.format(start_index, start_index+chunk_size))
        fetched_total_count = 0
        while True:
            records = cursor.fetchmany(size=MAX_CURSOR_LIMIT)
            if not records:
                break
            fetched_total_count = fetched_total_count + len(records)
            print("Fetched wallet-wallet edges count - {}".format(fetched_total_count))
            for address in records:
                create_edge(address[0], address[1], address[2], address[3], address[4], address[5], address[6])
            write_wallet_edges()
        cursor.close()
        start_index = start_index + chunk_size

    # rename collections
    arango_post_actions()

    # close arangodb connection
    close_gp_connection()


if __name__ == "__main__":
    main()
