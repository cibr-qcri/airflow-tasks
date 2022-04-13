import sys
import psycopg2
from psycopg2 import Error
import pickle
from pathlib import Path
import os
import csv
import logging

gp_connection = None
gp_cursor = None
volume_mount_path = '/opt/airflow/dags/'

# Data structures for heuristic-1 clustering
address_wallet_map = dict()
wallet_to_wallet_map = dict()
wallet_final_state_map = dict()
wallet_temp_map = dict()

def connects_to_greenplum():
    try:
        # Connect to an existing database
        global gp_connection
        gp_connection = psycopg2.connect(user=os.getenv('GREENPLUM_USERNAME'),
                                    password=os.getenv('GREENPLUM_PASSWORD'),
                                    host=os.getenv('GREENPLUM_HOST'),
                                    port=os.getenv('GREENPLUM_PORT'),
                                    database=os.getenv('GREENPLUM_DB'))

        # Create a cursor to perform database operations
        global gp_cursor
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute("SELECT version();")
        record = gp_cursor.fetchone()
        logging.info("You are connected to - ", record, "\n")
        return

    except (Exception, Error) as error:
        sys.exit("Error while connecting to PostgreSQL", error)

def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            logging.info("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        logging.info("Error while closing the connection to PostgreSQL", error)

def save_wallet_data():
    # save data in a csv file
    file_name = volume_mount_path + "address_wallet_mapping.csv"
    with open(file_name, 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in address_wallet_map.items():
            writer.writerow([key, value])
    logging.info("Multi address clustering - wallet mapping successfully write into a file: ", file_name)  

    with open(volume_mount_path + 'wallet_final_state_map.pickle', 'wb') as f:
        pickle.dump(wallet_final_state_map, f, pickle.HIGHEST_PROTOCOL)

def load_wallet_data(dict_name):
    wallet_file = Path(volume_mount_path + dict_name + ".pickle")
    if wallet_file.exists():
        with open(volume_mount_path + dict_name + '.pickle', 'rb') as f:
            return pickle.load(f)
    else:
        if dict_name != 'wallet_final_state_map':
            sys.exit(dict_name + " file can not be non exists")
        else:
            return dict()

def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()

def clear_data():
    address_wallet_map.clear()
    wallet_to_wallet_map.clear()
    wallet_final_state_map.clear()
    wallet_temp_map.clear()

def post_process_wallet_data():
    count = 0
    for key,value in address_wallet_map.items():
        wallet_id = value
        while True:
            # check wallet_id is already traversed and detect the final wallet_id
            cached_wallet_final_id = wallet_final_state_map.get(wallet_id)
            if cached_wallet_final_id is not None:
                wallet_id = cached_wallet_final_id

            temp_final_wallet_id = wallet_temp_map.get(wallet_id)
            if temp_final_wallet_id is not None:
                wallet_id = temp_final_wallet_id

            # check final traversal wallet_id for the given wallet_id
            traverse_wallet_id = wallet_to_wallet_map.get(wallet_id)
            if traverse_wallet_id is None:
                wallet_final_state_map[value] = wallet_id
                break
            else:
                wallet_id = traverse_wallet_id
        
        # update new wallet_id
        address_wallet_map[key] = wallet_id
        count = count + 1

        if count % 100000 == 0:
            logging.info("Processed another 100000 input address, total: ", count)

    logging.info("Multi address clustering - wallet mapping completed successfully")

def main():
    logging.info("Script started to merge all cluster ids for all clustered input addresses")

    # remove previous csv file if exists
    clustered_csv = Path(volume_mount_path + "address_wallet_mapping.csv")
    if clustered_csv.exists():
        os.remove(clustered_csv)

    global address_wallet_map 
    address_wallet_map = load_wallet_data('address_wallet_map')
    logging.info('Loaded {0} address_wallet_map entries to the memory'.format(len(address_wallet_map)))

    global wallet_to_wallet_map
    wallet_to_wallet_map = load_wallet_data('wallet_to_wallet_map')
    logging.info('Loaded {0} wallet_to_wallet_map entries to the memory'.format(len(wallet_to_wallet_map)))

    global wallet_temp_map
    wallet_temp_map = load_wallet_data('wallet_temp_map')
    logging.info('Loaded {0} wallet_temp_map entries to the memory'.format(len(wallet_temp_map)))

    global wallet_final_state_map 
    wallet_final_state_map = load_wallet_data('wallet_final_state_map')
    if wallet_final_state_map != None:
        logging.info('Loaded {0} wallet_final_state_map entries to the memory'.format(len(wallet_final_state_map)))
    
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    # process wallet mapping and identify final wallet_id for the addresses
    post_process_wallet_data()

    # close arangodb connection
    close_gp_connection()

    # save wallet maps in file system
    save_wallet_data()

    # clear all loaded in-memory data
    clear_data()

    # print_wallet_data_structure()
    logging.info("Multi address cluster-mapping process completed successfully")

if __name__ == "__main__":
    main()
