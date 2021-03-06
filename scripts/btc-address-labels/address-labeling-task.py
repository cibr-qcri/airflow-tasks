import sys
import psycopg2
from psycopg2 import Error
import csv
import os
import pickle
import time
from pathlib import Path
from elasticsearch import Elasticsearch
import urllib.parse

es = Elasticsearch(
    ['http://es.cibr.qcri.org:80'],
    max_retries=10, retry_on_timeout=True
)
gp_connection = None
gp_cursor = None
last_timestamp = 0
volume_mount_path = '/opt/airflow/dags/'
STEP_SIZE = 10000
DELIMITER = ','

# Data structures to store labels
document_id_map = dict()
darkweb_labels = list()
walletexplorer_labels = list()
twitter_labels = list()
bitcointalk_labels = list()
bitcoinabuse_labels = list()
splcenter_labels = list()
github_labels = list()
graphsense_labels = list()

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

def load_wallet_data(dict_name):
    wallet_file = Path(volume_mount_path + dict_name + ".pickle")
    if wallet_file.exists():
        with open(volume_mount_path + dict_name + '.pickle', 'rb') as f:
            return pickle.load(f)
    else:
        return dict()

def load_last_processed_timestamp():
    try:
        if Path(volume_mount_path + "last_label_processed_timestamp.pickle").exists():
            last_processed_input_data_map = load_wallet_data('last_label_processed_timestamp')
            last_timestamp = last_processed_input_data_map['last_timestamp']
            if last_timestamp is not None:
                return int(last_timestamp)
    except:
        pass

    return 0

def save_processed_timestamp():
    last_processed_timestamp = dict()
    last_processed_timestamp['last_timestamp'] = time.time()
    with open(volume_mount_path + 'last_label_processed_timestamp.pickle', 'wb') as f:
        pickle.dump(last_processed_timestamp, f, pickle.HIGHEST_PROTOCOL)

def load_darkweb_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            for row in response['_source']['data']['info']['cryptocurrency']['btc']:
                current = list()
                current.append(row['address'])
                current.append(str(response['_source']['data']['info']['domain']))
                current.append("Service > Darkweb > " + response['_source']['data']['info']['domain_info']['category']['type'].capitalize())
                current.append("dizzy.cibr.qcri.org")
                current.append(response['_source']['data']['timestamp'])
                current.append(urllib.parse.quote(response['_source']['data']['info']['url']))
                darkweb_labels.append(current)
            document_id_map[document_id] = "added"

def load_walletexplorer_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            current = list()
            current.append(response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc'])
            current.append(response['_source']['data']['info']['tags']['wallet']['name'])
            current.append(response['_source']['data']['info']['tags']['wallet']['category'])
            current.append("walletexplorer.com")
            current.append(response['_source']['data']['timestamp'])
            current.append(urllib.parse.quote(response['_source']['data']['info']['tags']['wallet']['url']))
            walletexplorer_labels.append(current)
            document_id_map[document_id] = "added"

def load_twitter_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            for address in response['_source']['info']['tags']['cryptocurrency']['address']['btc']:
                current = list()
                current.append(address)
                current.append(response['_source']['info']['tags']['actor']['preferred_username'].replace("\\", ""))
                current.append("User")
                current.append("twitter.com")
                current.append(response['_source']['timestamp'])
                current.append(urllib.parse.quote(response['_source']['info']['url']))
                twitter_labels.append(current)
            document_id_map[document_id] = "added"

def load_bitcointalk_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            for address in response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc']:
                names = response['_source']['data']['info']['tags']['profile']['name'].split(',')
                for name in names:
                    current = list()
                    current.append(address)
                    current.append(name.replace("\\", ""))
                    current.append("User")
                    current.append("bitcointalk.org")
                    current.append(response['_source']['data']['timestamp'])
                    current.append(urllib.parse.quote(response['_source']['data']['info']['url']))
                    bitcointalk_labels.append(current)
            document_id_map[document_id] = "added"

def load_bitcoinabuse_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            abusers = response['_source']['data']['info']['tags']['abuse']['report']['abuser'].split(',')
            for abuser in abusers:
                current = list()
                address = response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc']
                if len(address) <= 64:
                    current.append(response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc'])
                    current.append(abuser.replace("\\", ""))
                    current.append(response['_source']['data']['info']['tags']['abuse']['report']['category'])
                    current.append("bitcoinabuse.com")
                    current.append(response['_source']['data']['timestamp'])
                    current.append(urllib.parse.quote(response['_source']['data']['info']['url']))
                    bitcoinabuse_labels.append(current)
            document_id_map[document_id] = "added"

def load_splcenter_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            current = list()
            current.append(response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc'])
            current.append(response['_source']['data']['info']['tags']['report']['report']['label'])
            current.append(response['_source']['data']['info']['tags']['report']['report']['category'])
            current.append("splcenter.org")
            current.append(response['_source']['data']['timestamp'])
            current.append(response['_source']['data']['info']['tags']['report']['report']['note'])
            splcenter_labels.append(current)
            document_id_map[document_id] = "added"

def load_github_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            current = list()
            current.append(response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc'])
            current.append(response['_source']['data']['info']['tags']['repository']['name'])
            current.append(response['_source']['data']['info']['tags']['repository']['category'])
            current.append("github.com")
            current.append(response['_source']['data']['timestamp'])
            current.append(response['_source']['data']['info']['tags']['repository']['note'].replace(",", ""))
            github_labels.append(current)
            document_id_map[document_id] = "added"

def load_graphsense_labels(resp):
    for response in resp['hits']['hits']:
        document_id = response['_id']
        is_exists =  document_id_map.get(document_id)
        if is_exists is None:
            current = list()
            current.append(response['_source']['data']['info']['tags']['cryptocurrency']['address']['btc'])
            current.append(response['_source']['data']['info']['tags']['label'])
            current.append(response['_source']['data']['info']['tags']['category'])
            current.append("graphsense.info")
            current.append(response['_source']['data']['timestamp'])
            current.append(response['_source']['data']['info']['tags']['note'])
            graphsense_labels.append(current)
            document_id_map[document_id] = "added"

def store_csv(file_name, label_list):
    csv_file = volume_mount_path + file_name
    with open(csv_file, 'w') as file:
        writer = csv.writer(file, delimiter=DELIMITER)
        writer.writerows(label_list)

def get_darkweb_labels():
    resp = es.search(index="darkweb-tor-index",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "exists": {
                            "field": "data.info.domain_info.safety.is_safe"
                        }
                    },
                    {
                        "exists": {
                            "field": "data.info.cryptocurrency.btc.address"
                        }
                    },
                    {
                        "exists": {
                            "field": "data.info.domain_info.category.type"
                        }
                    },
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_darkweb_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_darkweb_labels(resp)      
    except:
        pass

    # store labels
    if len(darkweb_labels) > 0:
        print("processed label count: " + str(len(darkweb_labels)))
        store_csv('darkweb_labels.csv', darkweb_labels)
    darkweb_labels.clear()
    document_id_map.clear()

def get_walletexplorer_labels():
    resp = es.search(index="cibr-walletexplorer",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_walletexplorer_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_walletexplorer_labels(resp)      
    except:
        pass

    # store labels
    if len(walletexplorer_labels) > 0:
        print("processed label count: " + str(len(walletexplorer_labels)))
        store_csv('walletexplorer_labels.csv', walletexplorer_labels)
    walletexplorer_labels.clear()
    document_id_map.clear()

def get_twitter_labels():
    resp = es.search(index="twitter-crawler",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_twitter_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_twitter_labels(resp)      
    except:
        pass

    # store labels
    if len(twitter_labels) > 0:
        print("processed label count: " + str(len(twitter_labels)))
        store_csv('twitter_labels.csv', twitter_labels)
    twitter_labels.clear()
    document_id_map.clear()

def get_bitcointalk_labels():
    resp = es.search(index="bitcointalk-crawler",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "exists": {
                            "field": "data.info.tags.cryptocurrency.address.btc"
                        }
                    },
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_bitcointalk_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_bitcointalk_labels(resp)      
    except:
        pass

    # store labels
    if len(bitcointalk_labels) > 0:
        print("processed label count: " + str(len(bitcointalk_labels)))
        store_csv('bitcointalk_labels.csv', bitcointalk_labels)
    bitcointalk_labels.clear()
    document_id_map.clear()

def get_bitcoinabuse_labels():
    resp = es.search(index="cibr-bitcoinabuse",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_bitcoinabuse_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_bitcoinabuse_labels(resp)      
    except:
        pass

    # store labels
    if len(bitcoinabuse_labels) > 0:
        print("processed label count: " + str(len(bitcoinabuse_labels)))
        store_csv('bitcoinabuse_labels.csv', bitcoinabuse_labels)
    bitcoinabuse_labels.clear()
    document_id_map.clear()

def get_splcenter_labels():
    resp = es.search(index="cibr-splcenter",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_splcenter_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_splcenter_labels(resp)      
    except:
        pass

    # store labels
    if len(splcenter_labels) > 0:
        print("processed label count: " + str(len(splcenter_labels)))
        store_csv('splcenter_labels.csv', splcenter_labels)
    splcenter_labels.clear()
    document_id_map.clear()

def get_github_labels():
    resp = es.search(index="cibr-github",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_github_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_github_labels(resp)      
    except:
        pass

    # store labels
    if len(github_labels) > 0:
        print("processed label count: " + str(len(github_labels)))
        store_csv('github_labels.csv', github_labels)
    github_labels.clear()
    document_id_map.clear()

def get_graphsense_labels():
    resp = es.search(index="cibr-graphsense",body={
        "size": STEP_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "data.timestamp": {
                                "gte": last_timestamp
                            }
                        }
                    }
                ]
            }
        }
    }, scroll='1m')

    scroll_id = resp['_scroll_id']
    load_graphsense_labels(resp)

    try:
        for step in range(STEP_SIZE, resp['hits']['total']['value'], STEP_SIZE):
            resp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = resp['_scroll_id']
            load_graphsense_labels(resp)      
    except:
        pass

    # store labels
    if len(graphsense_labels) > 0:
        print("processed label count: " + str(len(graphsense_labels)))
        store_csv('graphsense_labels.csv', graphsense_labels)
    graphsense_labels.clear()
    document_id_map.clear()

def export_csv(file_name):
    print("Exporting csv data in : " + file_name)
    reader = open(file_name, 'r')
    gp_cursor.copy_from(reader, 'btc_address_label', sep=DELIMITER, columns=['address', 'label', 'category', 'source', 'timestamp', 'note'])
    reader.close()
    gp_connection.commit()

def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    error_message = None
    try:
        # apply tmp table schema in GP
        apply_sql_query(open("dependencies/address_label_table_schema.sql", "r").read())

        # remove existing indexes
        call_procedure("remove_btc_address_label_indexes")

        # load last processed timestamp
        global last_timestamp
        last_timestamp = load_last_processed_timestamp()

        # get darkweb labels
        print("Processing darkweb labels.")
        file = volume_mount_path + "darkweb_labels.csv"
        darkweb_csv = Path(file)
        if darkweb_csv.exists():
            os.remove(darkweb_csv)
        get_darkweb_labels();
        if darkweb_csv.exists():
            export_csv(file)

        # get walletexplorer labels
        print("Processing walletexplorer labels.")
        file = volume_mount_path + "walletexplorer_labels.csv"
        walletexplorer_csv = Path(file)
        if walletexplorer_csv.exists():
            os.remove(walletexplorer_csv)
        get_walletexplorer_labels();
        if walletexplorer_csv.exists():
            export_csv(file)

        # get twitter labels
        print("Processing twitter labels.")
        file = volume_mount_path + "twitter_labels.csv"
        twitter_csv = Path(file)
        if twitter_csv.exists():
            os.remove(twitter_csv)
        get_twitter_labels();
        if twitter_csv.exists():
            export_csv(file)

        # get bitcointalk labels
        print("Processing bitcointalk labels.")
        file = volume_mount_path + "bitcointalk_labels.csv"
        bitcointalk_csv = Path(file)
        if bitcointalk_csv.exists():
            os.remove(bitcointalk_csv)
        get_bitcointalk_labels();
        if bitcointalk_csv.exists():
            export_csv(file)

        # get bitcoinabuse labels
        print("Processing bitcoinabuse labels.")
        file = volume_mount_path + "bitcoinabuse_labels.csv"
        bitcoinabuse_csv = Path(file)
        if bitcoinabuse_csv.exists():
            os.remove(bitcoinabuse_csv)
        get_bitcoinabuse_labels();
        if bitcoinabuse_csv.exists():
            export_csv(file)

        # get splcenter labels
        print("Processing splcenter labels.")
        file = volume_mount_path + "splcenter_labels.csv"
        splcenter_csv = Path(file)
        if splcenter_csv.exists():
            os.remove(splcenter_csv)
        get_splcenter_labels();
        if splcenter_csv.exists():
            export_csv(file)

        # get github labels
        print("Processing github labels.")
        file = volume_mount_path + "github_labels.csv"
        github_csv = Path(file)
        if github_csv.exists():
            os.remove(github_csv)
        get_github_labels();
        if github_csv.exists():
            export_csv(file)

        # get graphsense labels
        print("Processing graphsense labels.")
        file = volume_mount_path + "graphsense_labels.csv"
        graphsense_csv = Path(file)
        if graphsense_csv.exists():
            os.remove(graphsense_csv)
        get_graphsense_labels();
        if graphsense_csv.exists():
            export_csv(file)

        print("Applying indexes and cluster ids for btc_address_label table")
        call_procedure("enrich_btc_address_label")

    except Exception as e:
        error_message = str(e)

    # close arangodb connection
    close_gp_connection()

    if error_message is not None:
        print(error_message)
        sys.exit(error_message)

    # save last processed timestamp
    save_processed_timestamp()

if __name__ == "__main__":
    main()
