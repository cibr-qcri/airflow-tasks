import psycopg2
from psycopg2 import Error
from pathlib import Path

gp_connection = None
gp_cursor = None
last_processed_input_id = 0
processing_row_count = 10000000
last_processed_tx_hash = None
last_processed_tx_wallet_id = None

# Data structures for heuristic-1 clustering
address_wallet_map = dict()
wallet_to_wallet_map = dict()
wallet_final_state_map = dict()
wallet_temp_map = dict()

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
        print("Error while connecting to PostgreSQL", error)

def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()


def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()
    
    query = "SELECT id, tx_hash, address, tx_value from btc_tx_input where id > {} and id <= {} order by id asc;".format(0, 10)
    print(query)
    tx_inputs = execute_sql_query(query)
    fetched_tx_count = len(tx_inputs)
    print(fetched_tx_count)

if __name__ == "__main__":
    main()