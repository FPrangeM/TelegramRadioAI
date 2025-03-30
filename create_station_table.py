from bs4 import BeautifulSoup
import pandas as pd
from curl_cffi import requests as cc_requests
import sqlite3
import asyncio
from curl_cffi import AsyncSession
from utils import my_custom_request


### Iterar sobre o url,codigo de cada pais e coletar suas respectivas radios e url_api

# Carregar relação country-url
with sqlite3.connect('project.db') as con:
    query = "SELECT country, url, code from countries"
    df_countries = pd.read_sql_query(query, con)

# Iterar para cada record, e coletar suas respectivas radios e url_api
# record = df_countries.iloc[0]



# Coletar estações de cada pais

async def fetch_station_data(record,session):
    record = record._asdict()
    
    print(f'Inicido: {record['country']}')
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "*/*",
      "Accept-Encoding": "gzip, deflate, br",
    }
    response = await session.get(record['url'], headers=headers)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.content, features="html.parser")

    station_el = soup.find('ul',id='radios').find_all('li')
    station_el = [item for item in station_el if 'hidden' not in item.get('class', [])]
    
    station_url = [est.a['href'].replace('#','') for est in station_el]
    station_names = [est.split('/')[-1] for est in station_url]
    station_api_urls = [f'https://api.instant.audio/data/playlist/{record['code']}/{station_name}' for station_name in station_names]

    estations_data = [{'country':record['country'],'station_name':name,'url':url,'api_url':api_url} for name,url,api_url in zip(station_names,station_url,station_api_urls)]
    print(f'{record['country']} fetched')
    return estations_data

async def main(df_countries):
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_station_data(record, session) for record in df_countries.itertuples()]
        results = await asyncio.gather(*tasks)

    estations = []
    for r in results:
        estations.extend(r)

    return estations



stations_data = asyncio.run(main(df_countries))
# stations_data = await main(df_countries)

df_stations = pd.DataFrame(stations_data)

con = sqlite3.connect("project.db")
cur = con.cursor()

# Salvar no banco de dados e na pasta local
cur.execute(f"create table stations({','.join(df_stations.columns)})")
con.commit()
cur.executemany("INSERT INTO stations VALUES(?,?,?,?)", df_stations.values)
con.commit()

df_stations.to_csv('Data/stations.csv',index=False)

con.close()