from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import asyncio
from curl_cffi import AsyncSession



### Iterar sobre o todas as url_api coletando as tracks recentes de cada stação

# Carregar relação country-url
with sqlite3.connect('project.db') as con:
    query = "SELECT DISTINCT api_url from stations"
    df_api_urls = pd.read_sql_query(query, con)


# Coletar json de cada station
async def fetch_station_json(record,session):
    record = record._asdict()
    
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "*/*",
      "Accept-Encoding": "gzip, deflate, br",
    }
    response = await session.get(record['api_url'], headers=headers)
    if response.status_code != 200:
        return None

    return response.json().get('result',[])

async def main(df_api_urls):
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_station_json(record, session) for record in df_api_urls.itertuples()]
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



con = sqlite3.connect("project.db")
cur = con.cursor()

# Salvar no banco de dados e na pasta local
# cur.execute(f"create table tracks({','.join(df_tracks.columns)})")
# con.commit()

cur.execute("DELETE FROM tracks")
con.commit()
cur.executemany("INSERT INTO tracks VALUES(?,?,?,?,?)", df_tracks.values)
con.commit()
cur.execute("""
    INSERT OR IGNORE INTO tracks_consolidada
    SELECT * FROM tracks
    EXCEPT
    SELECT * FROM tracks_consolidada""")
con.commit()

# df_tracks.to_csv('Data/tracks.csv',index=False)
# df_tracks.to_parquet('Data/tracks.parquet',index=False)
# df_tracks.to_feather('Data/tracks.feather')

con.close()




