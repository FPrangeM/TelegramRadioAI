from bs4 import BeautifulSoup
import pandas as pd
from curl_cffi import requests as cc_requests
import sqlite3
import asyncio
from curl_cffi import AsyncSession
from utils import my_custom_request
from sqlalchemy import create_engine
import utils

### Iterar sobre o url,codigo de cada pais e coletar suas respectivas radios e url_api

# Carregar relação country-url
engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/radio_db")
with engine.connect() as conn:
    query = "SELECT country, url, code from countries"
    df_countries = pd.read_sql_query(query, conn)

# Iterar para cada record, e coletar suas respectivas radios e url_api
# record = df_countries.iloc[0]

# Coletar estações de cada pais

async def fetch_station_data(record, session, max_retries=3, retry_delay=5):
    record = record._asdict()
    
    print(f'Iniciado: {record["country"]}')
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }

    for attempt in range(max_retries):
        try:
            response = await session.get(record['url'], headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Status code {response.status_code}")
                
            soup = BeautifulSoup(response.content, features="html.parser")
            station_el = soup.find('ul', id='radios').find_all('li')
            station_el = [item for item in station_el if 'hidden' not in item.get('class', [])]
            
            station_url = [est.a['href'].replace('#', '') for est in station_el]
            station_names = [est.split('/')[-1] for est in station_url]
            station_api_urls = [f'https://api.instant.audio/data/playlist/{record["code"]}/{station_name}' 
                              for station_name in station_names]

            stations_data = [{
                'country': record['country'],
                'station_name': name,
                'url': url,
                'api_url': api_url
            } for name, url, api_url in zip(station_names, station_url, station_api_urls)]
            
            print(f'{record["country"]} fetched')
            return stations_data
            
        except Exception as e:
            if attempt == max_retries - 1:  # Última tentativa
                print(f'Falha após {max_retries} tentativas para {record["country"]}: {str(e)}')
                return None
                
            print(f'Tentativa {attempt + 1} falhou para {record["country"]}. Tentando novamente em {retry_delay} segundos...')
            await asyncio.sleep(retry_delay)

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

# Salvar no banco de dados
utils.bulk_insert_dataframe(df_stations,'stations',engine)
