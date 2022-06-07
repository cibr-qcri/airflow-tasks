import io
import os

from messari.defillama import DeFiLlama
from sqlalchemy import create_engine


def fetch_protocols():
    dl = DeFiLlama()
    return dl.get_protocols().T[data]


def persist():
    db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}' \
        .format(os.getenv('GREENPLUM_USERNAME'), os.getenv('GREENPLUM_PASSWORD'),
                os.getenv('GREENPLUM_HOST'), os.getenv('GREENPLUM_PORT'), os.getenv('GREENPLUM_DEFI_DB'))
    table_name = 'defilama_protocol'
    engine = create_engine(db_url)
    protocols.head(0).to_sql(table_name, engine, if_exists='replace', index=True)

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    protocols.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null='')
    conn.commit()


if __name__ == '__main__':
    data = ['name', 'address', 'symbol', 'url', 'description', 'chain', 'gecko_id',
            'cmcId', 'twitter', 'category', 'chains']
    protocols = fetch_protocols()
    persist()
