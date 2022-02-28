import redis
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
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


with DAG(
        dag_id='scripts',
        schedule_interval='@daily',
        catchup=False,
        tags=['darkweb', 'healthcheck'],
) as dag:
    @task(task_id="push-domains")
    def push_domains():
        es_conn = BaseHook.get_connection("elasticsearch")
        redis_conn = BaseHook.get_connection("crawlers-redis")

        es = Elasticsearch([f"{es_conn.host}:{es_conn.port}"], timeout=50, max_retries=10, retry_on_timeout=True,
                           sniff_on_start=True)
        r = redis.Redis(host=redis_conn.host, port=redis_conn.port, db=0)

        #clear_queues(r)
        domains = get_domains(es)
        add_start_urls(r, domains)
