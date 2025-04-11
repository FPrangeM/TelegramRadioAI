from curl_cffi import requests as cc_requests
import sqlite3
import pandas as pd
import sqlite3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from sqlalchemy import create_engine, text
import datetime
import io

def my_custom_request(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }

    response = cc_requests.get(url, headers=headers, impersonate="chrome120")
    return response


# Funções para o PostgreSQL

def check_table_exists(table_name,engine):

    with engine.connect() as conn:
        result = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """))
    return result.fetchone()[0]


def create_table_from_dataframe(df,table_name,engine,partition_column=None):
    df[:0].to_sql(table_name, engine, index=False)

    if partition_column:
        with engine.connect() as conn:
            conn.execute(text(f"""
                SELECT create_hypertable(
                    '{table_name}',
                    '{partition_column}',
                    chunk_time_interval => INTERVAL '1 day',
                    if_not_exists => TRUE
                );
            """))
        conn.commit()


def bulk_insert_dataframe(df, table_name, engine, batch_size=500000):
    if not check_table_exists(table_name, engine):
        create_table_from_dataframe(df, table_name, engine)
    
    # Remover caracteres nulos e problemáticos
    df = df.map(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
    
    total_rows = len(df)
    num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        
        buffer = io.StringIO()
        batch_df.to_csv(buffer, sep='\t', header=False, index=False)
        buffer.seek(0)

        with engine.connect() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            try:
                cursor.copy_expert(
                    f"""
                    COPY {table_name} ({', '.join(df.columns)})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
                    """,
                    buffer
                )
                raw_conn.commit()
            except Exception as e:
                raw_conn.rollback()
                raise e
            finally:
                cursor.close()