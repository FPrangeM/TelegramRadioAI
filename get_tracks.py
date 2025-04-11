from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import asyncio
from curl_cffi import AsyncSession
from sqlalchemy import create_engine,text
import utils
from tqdm.asyncio import tqdm_asyncio
import asyncio

### Iterar sobre o todas as url_api coletando as tracks recentes de cada stação

# Carregar relação country-url
engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/radio_db")
with engine.connect() as conn:
    query = "SELECT DISTINCT api_url from stations"
    df_api_urls = pd.read_sql_query(query, conn)

max_index = df_api_urls.index.stop

# Coletar json de cada station
async def fetch_station_json(record, session, max_retries=5, retry_delay=5, pbar=None):
    record = record._asdict()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }

    for attempt in range(max_retries):
        try:
            response = await session.get(record['api_url'], headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Status code {response.status_code}")
                
            json_data = response.json()
            if pbar:
                pbar.update(1)  # Atualiza a barra de progresso
            return json_data.get('result', [])
            
        except Exception as e:
            if attempt == max_retries - 1:  # Última tentativa
                print(f'Falha após {max_retries} tentativas para {record.get("api_url", "Unknown")}: {str(e)}')
                if pbar:
                    pbar.update(1)  # Atualiza mesmo em caso de falha
                return None
                
            print(f'Tentativa {attempt + 1} falhou para {record.get("api_url", "Unknown")}. Tentando novamente em {retry_delay} segundos...')
            await asyncio.sleep(retry_delay)


async def main(df_api_urls):
    async with AsyncSession(impersonate="chrome120") as session:
        # Cria a barra de progresso
        with tqdm_asyncio(total=len(df_api_urls), desc="Fetching stations") as pbar:
            tasks = [fetch_station_json(record, session, pbar=pbar) for record in df_api_urls.itertuples()]
            results = await asyncio.gather(*tasks)

    records = []
    for record, tracks in zip(df_api_urls.itertuples(), results):
        record = record._asdict()
        if not tracks:
            continue
            
        api_url = record['api_url']
        
        for track in tracks:
            records.append({
                'playlist_url': api_url,
                **track
            })

    return records


tracks = asyncio.run(main(df_api_urls))
# tracks = await main(df_api_urls)


df_tracks=pd.DataFrame(tracks)





if not utils.check_table_exists('tracks',engine):
    utils.create_table_from_dataframe(df_tracks,'tracks',engine)

if not utils.check_table_exists('tracks_consolidada',engine):
    df_tracks_dth=df_tracks.loc[:0].copy()
    df_tracks_dth['track_played'] = pd.to_datetime(df_tracks_dth['track_played'], unit='s')
    utils.create_table_from_dataframe(df_tracks_dth[:0],'tracks_consolidada',engine,partition_column='track_played')



# Salvar no banco de dados
with engine.connect() as conn:
    conn.execute(text("DELETE FROM tracks"))
    conn.commit()

utils.bulk_insert_dataframe(df_tracks,'tracks',engine)



query_string = """INSERT INTO tracks_consolidada
(SELECT playlist_url, track_artist, track_title, track_image, to_timestamp(track_played) as track_played
FROM tracks
where to_timestamp(track_played) >= current_timestamp - interval '2 hour')
EXCEPT
(select 
* from tracks_consolidada
where date(track_played) >= current_date-1
and track_played >= current_timestamp - interval '2 hour')
ON CONFLICT DO NOTHING"""

with engine.connect() as conn:
    conn.execute(text(query_string))
    conn.commit()

