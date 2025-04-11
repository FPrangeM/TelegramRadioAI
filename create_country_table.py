from bs4 import BeautifulSoup
import pandas as pd
from curl_cffi import requests as cc_requests
import sqlite3
import asyncio
from curl_cffi import AsyncSession
from sqlalchemy import create_engine
import utils

# Conectar ao banco de dados Postgre
engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/radio_db")

# Configurações da requisição HTTP
url = "https://instant.audio"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
}

# Fazer a requisição HTTP
response = cc_requests.get(url, headers=headers, impersonate="chrome120")
soup = BeautifulSoup(response.content, features="html.parser")

# Encontrar todas as caixas de continentes ativas
continent_boxes = soup.find_all("div", {"class": "box active"})

countries_data = []

# Processar cada continente
for continent_box in continent_boxes:
    # Extrair nome do continente
    continent_name = continent_box.strong.a.text

    # Encontrar todos os países na lista do continente
    country_elements = continent_box.find("ul", {"class": "country-list"}).find_all("li")

    # Processar cada país
    for country_element in country_elements:
        countries_data.append({
            "continent": continent_name,
            "country": country_element.a.text,
            "url": country_element.a["href"]
        })


# Coletar codigo dos paises
async def fetch_country_code(country_data, session, max_retries=3, retry_delay=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }
    
    print(f'Iniciado: {country_data["country"]}')
    
    for attempt in range(max_retries):
        try:
            response = await session.get(country_data['url'], headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Status code {response.status_code}")
                
            soup = BeautifulSoup(response.content, features="html.parser")
            el = soup.find('link', {'as':"fetch", 'type':"application/json", 'crossorigin':"anonymous"})
            
            if not el:
                raise Exception("Elemento com código do país não encontrado")
                
            country_code = el['href'].split('streams/')[1].split('/')[0]
            
            print(f'Coleta finalizada: {country_data["country"]}')
            return country_code
            
        except Exception as e:
            if attempt == max_retries - 1:  # Última tentativa
                print(f'Falha após {max_retries} tentativas para {country_data["country"]}: {str(e)}')
                return None
                
            print(f'Tentativa {attempt + 1} falhou para {country_data["country"]}. Tentando novamente em {retry_delay} segundos...')
            await asyncio.sleep(retry_delay)

async def main(countries_data):
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_country_code(country_data, session) for country_data in countries_data]
        results = await asyncio.gather(*tasks)

    return [{'country': country['country'], 'code': code} for country, code in zip(countries_data, results)]


country_codes = asyncio.run(main(countries_data))
# country_codes = await main(countries_data)

for data, code in zip(countries_data, country_codes):
    data['code'] = code['code']


# Criar DataFrame com os dados coletados
df_countries = pd.DataFrame(countries_data)

# Salvar no banco de dados 
# utils.bulk_insert_dataframe(df_countries,'countries',engine)