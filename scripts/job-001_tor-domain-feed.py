import os

import redis
from elasticsearch import Elasticsearch


def clear_queues(conn):
    try:
        conn.delete('darkweb-healthcheck:dupefilter')
    except:
        pass

    try:
        conn.delete('darkweb-healthcheck:start_urls')
    except:
        pass

    try:
        conn.delete('darkweb-healthcheck:requests')
    except:
        pass


def add_start_urls(conn, domains):
    redis_key = 'darkweb-healthcheck:start_urls'
    conn.lpush(redis_key, *domains)


def unify(url):
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        pass
    else:
        url = "http://" + url
    return url.strip("/")


def get_domains(conn):
    aggs = {
        "domains": {
            "terms": {
                "field": "info.domain.keyword", "size": 50000
            }
        }
    }

    res = conn.search(index="tor-temp", body={
        "size": 0,
        "aggs": aggs,
    })

    results = [domain['key'] for domain in res['aggregations']['domains']['buckets']]

    return [unify(domain) for domain in results]


if __name__ == "__main__":
    es_host = os.getenv('ES_CONNECTION_HOST')
    es_port = os.getenv('ES_CONNECTION_PORT')
    redis_host = os.getenv('REDIS_CONNECTION_HOST')
    redis_port = os.getenv('REDIS_CONNECTION_PORT')

    es = Elasticsearch([f"{es_host}:{es_port}"], timeout=50, max_retries=10, retry_on_timeout=True)
    r = redis.Redis(host=redis_host, port=redis_port, db=0)

    clear_queues(r)
    domains = get_domains(es)
    add_start_urls(r, domains)
