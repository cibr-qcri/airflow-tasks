import sys
import psycopg2
from psycopg2 import Error
import uuid
import pickle
from pathlib import Path
import os

gp_connection = None
gp_cursor = None
last_processed_input_id = 0
processing_row_count = 10000000
last_processed_tx_hash = None
last_processed_tx_wallet_id = None
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


def multi_address__clustering_heuristic():

    global last_processed_input_id
    global last_processed_tx_hash
    global last_processed_tx_wallet_id

    total_addresses = execute_sql_query("SELECT max(id) from btc_tx_input;")
    start_index = last_processed_input_id
    end_index = int(total_addresses[0][0])

    while start_index <= end_index:
        print("Query input address range {} - {}".format(start_index, start_index+processing_row_count))

        # Gets all the input addresses stored
        query = "SELECT id, tx_hash, address, tx_value from btc_tx_input where id > {} and id <= {} order by id asc;".format(start_index, int(start_index + processing_row_count))
        print(query)
        tx_inputs = execute_sql_query(query)
        fetched_tx_count = len(tx_inputs)

        # check expected row count has been fetched
        if start_index+processing_row_count < end_index and fetched_tx_count != processing_row_count:
            sys.exit("Error while fetching input addresses from the databse. Hence abort the clustering process")

        print(len(tx_inputs), " - Input addresses loaded from the databse")

        generated_wallet_id = last_processed_tx_wallet_id
        count = 0
        for input_row in tx_inputs:
            id = input_row[0]
            tx_hash = input_row[1]
            input_address = input_row[2]

            if tx_hash != last_processed_tx_hash or generated_wallet_id is None:
                generated_wallet_id = str(uuid.uuid4())
                last_processed_tx_hash = tx_hash
                last_processed_tx_wallet_id = generated_wallet_id

            wallet_id = address_wallet_map.get(input_address)
            if wallet_id is None:
                address_wallet_map[input_address] = generated_wallet_id
            else:
                start_wallet_id = wallet_id
                temp_final_wallet_id = wallet_temp_map.get(start_wallet_id)
                if temp_final_wallet_id is not None:
                    wallet_id = temp_final_wallet_id
                
                while True:
                    traverse_wallet_id = wallet_to_wallet_map.get(wallet_id)
                    if traverse_wallet_id is None:
                        if wallet_id != generated_wallet_id:
                            wallet_to_wallet_map[wallet_id] = generated_wallet_id
                        
                        if start_wallet_id != generated_wallet_id:
                            wallet_temp_map[start_wallet_id] = generated_wallet_id
                        break
                    else:
                        wallet_id = traverse_wallet_id
                            
            
            last_processed_input_id = id
            count = count + 1

            if count % 100 == 0:
                print("Processed another 100 input address, total: ", count)
        start_index = start_index + processing_row_count

def save_wallet_data():
    with open(volume_mount_path + 'address_wallet_map.pickle', 'wb') as f:
        pickle.dump(address_wallet_map, f, pickle.HIGHEST_PROTOCOL)

    with open(volume_mount_path + 'wallet_to_wallet_map.pickle', 'wb') as f:
        pickle.dump(wallet_to_wallet_map, f, pickle.HIGHEST_PROTOCOL)

    with open(volume_mount_path + 'wallet_temp_map.pickle', 'wb') as f:
        pickle.dump(wallet_temp_map, f, pickle.HIGHEST_PROTOCOL) 
    
    last_processed_input_data_map = dict()
    last_processed_input_data_map['last_id'] = last_processed_input_id
    last_processed_input_data_map['last_tx_hash'] = last_processed_tx_hash
    last_processed_input_data_map['last_tx_wallet_id'] = last_processed_tx_wallet_id
    with open(volume_mount_path + 'last_processed_input_data.pickle', 'wb') as f:
        pickle.dump(last_processed_input_data_map, f, pickle.HIGHEST_PROTOCOL)

def load_wallet_data(dict_name):
    wallet_file = Path(volume_mount_path + dict_name + ".pickle")
    if wallet_file.exists():
        with open(volume_mount_path + dict_name + '.pickle', 'rb') as f:
            return pickle.load(f)
    else:
        return dict()  

def load_last_processed_input_metadata():
    try:
        if Path(volume_mount_path + "last_processed_input_data.pickle").exists():
            last_processed_input_data_map = load_wallet_data('last_processed_input_data')
            last_processed_input_id = last_processed_input_data_map['last_id']
            last_processed_tx_hash = last_processed_input_data_map['last_tx_hash']
            last_processed_tx_wallet_id = last_processed_input_data_map['last_tx_wallet_id']
            if last_processed_input_data_map is not None and last_processed_input_id is not None and last_processed_tx_hash is not None and last_processed_tx_wallet_id is not None:
                return int(last_processed_input_id), last_processed_tx_hash, last_processed_tx_wallet_id
    except:
        pass

    return 0, None, None

def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()

def clear_data():
    address_wallet_map.clear()
    wallet_to_wallet_map.clear()
    wallet_final_state_map.clear()
    wallet_temp_map.clear()

def main():
    # read previous run wallet metadata
    global last_processed_input_id
    global last_processed_tx_hash
    global last_processed_tx_wallet_id
    last_processed_input_id,last_processed_tx_hash,last_processed_tx_wallet_id = load_last_processed_input_metadata()

    # load wallet_to_wallet_map and wallet_address_map to the memory if exists
    if last_processed_input_id != 0:
        global address_wallet_map 
        address_wallet_map = load_wallet_data('address_wallet_map')
        print('Loaded {0} address_wallet_map entries to the memory'.format(len(address_wallet_map)))

        global wallet_to_wallet_map
        wallet_to_wallet_map = load_wallet_data('wallet_to_wallet_map')
        print('Loaded {0} wallet_to_wallet_map entries to the memory'.format(len(wallet_to_wallet_map)))

        global wallet_temp_map
        wallet_temp_map = load_wallet_data('wallet_temp_map')
        print('Loaded {0} wallet_temp_map entries to the memory'.format(len(wallet_temp_map)))

    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    # process clustering
    print("Script started to process input address id range: {} to {}".format(last_processed_input_id, last_processed_input_id + processing_row_count))
    multi_address__clustering_heuristic()

    # close arangodb connection
    close_gp_connection()

    # save wallet maps in file system
    save_wallet_data()

    # clear all loaded in-memory data
    clear_data()

    # print_wallet_data_structure()
    print("Multi address clustering process completed successfully")

if __name__ == "__main__":
    main()
