import sys
import psycopg2
from psycopg2 import Error
from pathlib import Path
import csv
import os
import networkx as nx

gp_connection = None
gp_cursor = None
volume_mount_path = ''
local_file_path = 'dependencies/'
MAX_CURSOR_LIMIT = 500000
G = nx.MultiDiGraph()
wallet_size_map = dict()

def connects_to_greenplum():
    try:
        # Connect to an existing database
        global gp_connection
        gp_connection = psycopg2.connect(user="gpadmin",
                                    password="",
                                    host="10.4.8.131",
                                    port="5432",
                                    database="btc_blockchain")

        # Create a cursor to perform database operations
        global gp_cursor
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute("SELECT version();")
        record = gp_cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return

    except (Exception, Error) as error:
        sys.exit("Error while connecting to PostgreSQL", error)

def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)

def apply_sql_query(query):
    gp_cursor.execute(query)
    gp_connection.commit()
    print("Record applied successfully ")

def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()

def call_procedure(procedure_name):
    gp_cursor.callproc(procedure_name)
    gp_connection.commit()
    return gp_cursor.fetchall()

def export_csv(file_name):
    print("Exporting clustering csv data in : " + file_name)
    reader = open(file_name, 'r')
    gp_cursor.copy_from(reader, 'tmp_btc_address_cluster', sep=',', columns=['address', 'cluster_id'])
    reader.close()
    gp_connection.commit()

def create_graph(wallet_in, wallet_out, tx_count, out_satoshi_amount, out_usd_amount):
    G.add_edge(wallet_in,wallet_out, tx_count=tx_count, out_satoshi_amount=out_satoshi_amount, out_usd_amount=out_usd_amount)

def fetch_wallet_edges():
    total_addresses = execute_sql_query("SELECT max(id) from tmp_btc_address_cluster;")
    print("Total addresses: ", total_addresses[0][0])
    start_index = 0
    end_index = int(total_addresses[0][0])
    chunk_size = 1000000

    while start_index <= end_index:
        print("Query wallet map for range {} - {}".format(start_index, start_index+chunk_size))
        cursor = gp_connection.cursor(name='fetch_large_result')
        cursor.execute("""
            select wallet_in, wallet_out, in_wallets.tx_hash as tx_hash, out_satoshi_amount, out_usd_amount from (
              SELECT cluster_id as wallet_out, btc_input_addresses.address as address, tx_hash, tx_value as out_satoshi_amount, usd_value as out_usd_amount 
              from (select address, cluster_id from tmp_btc_address_cluster where id >= {} and id < {} order by id asc) as btc_input_addresses 
              INNER JOIN btc_tx_input on btc_input_addresses.address=btc_tx_input.address
              ) as out_wallets
            inner join (
              SELECT cluster_id as wallet_in, btc_output_addresses.address as address, tx_hash, tx_value as in_satoshi_amount, usd_value as in_usd_amount 
              from (select address, cluster_id from tmp_btc_address_cluster where id >= {} and id < {} order by id asc) as btc_output_addresses 
              INNER JOIN btc_tx_output on btc_output_addresses.address=btc_tx_output.address 
            ) as in_wallets 
            on in_wallets.tx_hash=out_wallets.tx_hash;
            """.format(start_index, start_index+chunk_size, start_index, start_index+chunk_size))
            
        print("Query executed for range - {} - {}".format(start_index, start_index+chunk_size))
        fetched_total_count = 0
        while True:
            records = cursor.fetchmany(size=MAX_CURSOR_LIMIT)
            if not records:
                break
            fetched_total_count = fetched_total_count + len(records)
            print("Fetched wallet-wallet edges count - {}".format(fetched_total_count))

            with open(volume_mount_path + 'wallet-edges.csv', 'a', newline='') as source:  
                wtr = csv.writer(source)
                for record in records:
                    wtr.writerow((record[0], record[1], record[2], record[3], record[4]))
                source.close()

        cursor.close()
        start_index = start_index + chunk_size

    print("Wallet edges successfully write into a csv file")

def calculate_connected_nodes():
    print( "Found wallet count from the graph: " + str(len(G.nodes())))
    processed_nodes = 0
    for first_cluster in G.nodes():
        for second_cluster in G.neighbors(first_cluster):
            current = list()
            num_txes_to_second = 0
            num_txes_from_second = 0
            total_satoshi_to_second = 0
            total_satoshi_from_second = 0
            total_usd_to_second = 0
            total_usd_from_second = 0
            # find edges from first to second
            edge_list_to_second = G.get_edge_data(first_cluster, second_cluster)
            if edge_list_to_second is not None:
                num_txes_to_second = len(edge_list_to_second)
                for edge in edge_list_to_second:
                    total_satoshi_to_second += edge['out_satoshi_amount']
                    total_usd_to_second += edge['out_usd_amount']
            # find edges from second to first
            edge_list_from_second = G.get_edge_data(second_cluster, first_cluster)
            if edge_list_from_second is not None:
                num_txes_from_second = len(edge_list_from_second)
                for edge in edge_list_from_second:
                    total_satoshi_from_second += edge['out_satoshi_amount']
                    total_usd_from_second += edge['out_usd_amount']
            # construct row
            current.append(first_cluster)
            current.append(second_cluster)
            current.append(num_txes_to_second)
            current.append(num_txes_from_second)
            current.append(total_satoshi_to_second)
            current.append(total_satoshi_from_second)
            current.append(total_usd_to_second)
            current.append(total_usd_from_second)
            
            with open(volume_mount_path + 'wallet-wallet.csv', 'a', newline='') as source:  
                wtr = csv.writer(source)
                wtr.writerow(current)
                source.close()
        processed_nodes += 1
        if processed_nodes % 100000 == 0:
            print("Processed another 100000 wallet nodes, total: ", processed_nodes)
    G.clear()

def load_wallet_sizes():
    records = execute_sql_query("select cluster_id, num_address from tmp_btc_wallet;")
    for wallet in records:
        wallet_size_map[wallet[0]] = int(wallet[1])
    print("Wallets loaded with their address sizes")

def generate_linked_wallet_csv():
    with open(volume_mount_path + 'wallet-wallet.csv', "r") as source:
        rows = csv.reader( source )
        with open(volume_mount_path + 'linked-wallet.csv', "w") as result:
            writer = csv.writer( result )
            for r in rows:
                first_wallet = r[0]
                second_wallet = r[1]
                first_cluster_size = wallet_size_map.get(first_wallet)
                if first_cluster_size is None:
                    first_cluster_size = 0
                second_cluster_size = wallet_size_map.get(second_wallet)
                if second_cluster_size is None:
                    second_cluster_size = 0
                writer.writerow( (first_wallet, second_wallet, r[2], r[3], r[4], r[5], r[6], r[7], first_cluster_size, second_cluster_size))
            result.close()
        source.close()
    print("Final Link-Wallet data successfully write into a csv file")

def construct_graph():
    with open(volume_mount_path + 'wallet-edges.csv', "r") as source:
        row_count = 0
        for r in source:
            create_graph(r[0], r[1], r[2], r[3], r[4])
            if row_count % 50000000 == 0:
                print("Processed another 50000000 wallet-wallet edges, total: ", row_count)
            row_count += 1
    source.close()

    print("Wallet grapgh successfully generated")

def export_csv(file_name):
    print("Exporting csv data in : " + file_name)
    reader = open(file_name, 'r')
    gp_cursor.copy_from(reader, 'tmp_btc_link_wallet', sep=',', columns=['first_cluster', 'second_cluster', 'num_txes_to_second', 'num_txes_from_second', 'total_satoshi_to_second', 'total_satoshi_from_second', 'total_usd_to_second', 'total_usd_from_second', 'first_cluster_size', 'second_cluster_size'])
    reader.close()
    gp_connection.commit()

def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    error_message = None
    try:
        # fetch wallet-wallet edges store as a csv
        #fetch_wallet_edges()

        # construct wallet-wallet graph
        construct_graph()

        calculate_connected_nodes()
        print("Wallet-Wallet data successfully write into a csv file")

        # fetch wallet with sizes into memory
        load_wallet_sizes()

        # create final link_wallet CSV file
        generate_linked_wallet_csv()

        # export into GP
        export_csv(volume_mount_path + "linked-wallet.csv")
        print("tmp_btc_link_wallet filled with table successfully")

        # apply table indexes in GP
        # apply_sql_query(open(local_file_path + "cluster_table_index.sql", "r").read())

    except Exception as e:
        error_message = str(e)

    # close arangodb connection
    close_gp_connection()

    if error_message is not None:
        sys.exit(error_message)

if __name__ == "__main__":
    main()
