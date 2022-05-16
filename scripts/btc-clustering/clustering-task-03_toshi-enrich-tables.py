import sys
import psycopg2
from psycopg2 import Error
from pathlib import Path
import os

gp_connection = None
gp_cursor = None
volume_mount_path = '/opt/airflow/dags/'
local_file_path = 'dependencies/'

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

def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    error_message = None
    try:
        # apply tmp table schema in GP
        apply_sql_query(open(local_file_path + "cluster_tables_schema.sql", "r").read())

        # check previous csv file if exists
        clustered_csv = Path(volume_mount_path + "address_wallet_mapping.csv")
        if not clustered_csv.exists():
            sys.exit("enrich functions need clustered csv to proceed.")

        # insert address-cluster_id csv in GP
        export_csv(volume_mount_path + "address_wallet_mapping.csv")
        print("Clustered addressed inserted to the tmp_btc_address_cluster table")

        # insert enrich tmp_btc_address_cluster stored procedure in GP
        apply_sql_query(open(local_file_path + "cluster_address_enrich_procedures.sql", "r").read())
        call_procedure("enrich_tmp_btc_address_cluster_table")
        print("Procedure enrich_tmp_btc_address_cluster_table executed successfully")

        # insert enrich tmp_btc_wallet stored procedure in GP
        apply_sql_query(open(local_file_path + "cluster_wallet_enrich_procedures.sql", "r").read())
        call_procedure("enrich_tmp_btc_wallet_table")
        print("Procedure enrich_tmp_btc_wallet_table executed successfully")

        # insert enrich tmp_btc_wallet_transaction stored procedure in GP
        apply_sql_query(open(local_file_path + "cluster_tx_enrich_procedures.sql", "r").read())
        call_procedure("enrich_btc_wallet_transaction_table")
        print("Procedure enrich_btc_wallet_transaction_table executed successfully")

    except Exception as e:
        error_message = str(e)

    # close arangodb connection
    close_gp_connection()

    if error_message is not None:
        sys.exit(error_message)

if __name__ == "__main__":
    main()
