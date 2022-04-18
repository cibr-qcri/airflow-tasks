import csv
import statistics
from collections import Counter

import pandas as pd
import psycopg2
from tqdm import tqdm
from elasticsearch import Elasticsearch
from psycopg2 import Error

gp_connection = None
gp_cursor = None


def connects_to_greenplum():
    try:
        global gp_connection

        global gp_cursor
        gp_connection = psycopg2.connect(user="gpadmin", password="", host="10.4.8.131", port="5432",
                                         database="btc_blockchain")
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute("SELECT version();")
        gp_cursor.fetchone()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def fetch_bitcoinabuse_reports():
    reports = []
    with open('bitcoinabuse-reports.csv', "r", newline="") as file:
        reader = csv.reader(file, delimiter=',')
        for row in reader:
            if len(row) > 1:
                reports.append(row[1])

    reports = Counter(reports)
    for c in reports:
        reports[c] = 1 / (1 + (1 / reports[c]))

    return Counter(reports)


def fetch_safety():
    safety_scores = dict()

    def source_to_dict(source):
        info = source['data']['info']
        domain = info['domain_info']

        addresses = info['cryptocurrency']['btc']
        for address in addresses:
            return {'address': address['address'], 'is_safe': domain['safety']['is_safe'],
                    'score': domain['safety']['score']}

    step_size = 10000
    scroll_timeout = '5m'

    res = es.search(index='darkweb-tor-index', body={
        "size": step_size,
        "query": {
            "bool": {
                "filter": [
                    {
                        "exists": {
                            "field": "data.info.cryptocurrency.btc.address"
                        }
                    },
                    {
                        "exists": {
                            "field": "data.info.domain_info.safety"
                        }
                    }
                ]
            }
        },
        "track_total_hits": "true",
        "_source": {
            "includes": [
                "data.info.cryptocurrency.btc.address",
                "data.info.domain_info.safety.is_safe",
                "data.info.domain_info.safety.score"
            ]
        },
    }, scroll=scroll_timeout)

    results = [source_to_dict(i["_source"]) for i in res["hits"]["hits"]]
    scroll_id = res['_scroll_id']

    for step in range(step_size, res['hits']['total']['value'], step_size):
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
        scroll_id = res['_scroll_id']
        results.extend([source_to_dict(i["_source"]) for i in res["hits"]["hits"]])

    df = pd.DataFrame(results)
    df = df.loc[df['is_safe']]
    for name, group in df.groupby('address'):
        for score in group['score']:
            if name in safety_scores:
                safety_scores[name].append(score)
            else:
                safety_scores[name] = [score]
    for key in safety_scores:
        safety_scores[key] = statistics.mean(safety_scores[key])

    return safety_scores


def fetch_wallet(address):
    query = "SELECT cluster_id FROM btc_address_cluster WHERE address=\'{}\';".format(address)
    print(query)
    gp_cursor.execute(query)
    cluster_id = gp_cursor.fetchone()
    print(cluster_id)
    if cluster_id and len(cluster_id) > 0:
        return cluster_id[0]
    else:
        return None


def update_tohi_score(wallet, score):
    query = "UPDATE btc_wallet SET risk_score={} WHERE cluster_id='{}';".format(score, wallet)
    gp_cursor.execute(query)
    gp_connection.commit()


if __name__ == '__main__':
    connects_to_greenplum()
    es = Elasticsearch(['es.cibr.qcri.org'], scheme="http", port=80, timeout=50, max_retries=10, retry_on_timeout=True)
    bitcoinabuse_scores = fetch_bitcoinabuse_reports()
    safety_scores = fetch_safety()

    risk_scores = {}
    for k, v in tqdm(safety_scores.items()):
        if k in bitcoinabuse_scores:
            risk_scores[k] = (bitcoinabuse_scores[k] + safety_scores[k]) / 2
            del bitcoinabuse_scores[k]
        else:
            risk_scores[k] = v

    risk_scores.update(bitcoinabuse_scores)
    df = pd.DataFrame(risk_scores.items(), columns=['address', 'score'])
    df = df.loc[~df['address'].isin(['address'])]
    df['wallet'] = df['address'].apply(lambda x: fetch_wallet(x))
    df.dropna(inplace=True)
    df = df.sort_values('score', ascending=False).drop_duplicates('wallet').sort_index()

    for index, row in tqdm(df.iterrows()):
        update_tohi_score(row['wallet'], row['score'])
