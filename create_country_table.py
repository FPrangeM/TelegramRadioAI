from bs4 import BeautifulSoup
import pandas as pd
from curl_cffi import requests as cc_requests
import sqlite3
import asyncio
from curl_cffi import AsyncSession


# Coletar: continente, pais, url, codigo

# Conectar ao banco de dados SQLite
con = sqlite3.connect("project.db")
cur = con.cursor()

# Configurações da requisição HTTP
url = "https://instant.audio"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
async def fetch_station_json(record, session, max_retries=3, retry_delay=10):
    record = record._asdict()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }
    
    for attempt in range(max_retries):
        try:
            response = await session.get(record['api_url'], headers=headers)
            if response.status_code == 200:
                return response.json().get('result', [])
            
            # Se não for 200, trata como erro para tentar novamente
            raise Exception(f"Status code {response.status_code}")
            
        except Exception as e:
            if attempt == max_retries - 1:  # Última tentativa
                print(f"Falha após {max_retries} tentativas para {record['api_url']}: {str(e)}")
                return None
                
            print(f"Tentativa {attempt + 1} falhou para {record['api_url']}. Tentando novamente em {retry_delay} segundos...")
            await asyncio.sleep(retry_delay)


async def main(countries_data):
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_station_json(country_data, session) for country_data in countries_data]
        results = await asyncio.gather(*tasks)

    return [{'country': country['country'],'code': code} for country, code in zip(countries_data, results)]


country_codes = asyncio.run(main(countries_data))
# country_codes = await main(countries_data)

for data, code in zip(countries_data, country_codes):
    data['code'] = code['code']


# Criar DataFrame com os dados coletados
df_countries = pd.DataFrame(countries_data)

# Salvar no banco de dados e na pasta local
cur.execute(f"create table countries({','.join(df_countries.columns)})")
con.commit()
cur.executemany("INSERT INTO countries VALUES(?,?,?,?)", df_countries.values)
con.commit()

# df_countries.to_csv('Data/countries.csv',index=False)

con.close()