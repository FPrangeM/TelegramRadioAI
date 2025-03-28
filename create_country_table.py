from bs4 import BeautifulSoup
import requests
import time
from lxml import etree
import pandas as pd
from curl_cffi import requests as cc_requests
import datetime
import json
import os
import sqlite3
import asyncio
from curl_cffi import AsyncSession


# Coletar: continente, pais, url, codigo

# Conectar ao banco de dados SQLite
# conn = sqlite3.connect("tutorial.db")
# cursor = conn.cursor()

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
async def fetch_country_code(url, session):
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "*/*",
      "Accept-Encoding": "gzip, deflate, br",
    }
    response = await session.get(url, headers=headers)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.content, features="html.parser")
    el = soup.find('link',{'as':"fetch",'type':"application/json",'crossorigin':"anonymous"})
    country_code = el['href'].split('streams/')[1].split('/')[0]

    return country_code
    

async def main(countries_data):
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_country_code(country['url'], session) for country in countries_data]
        results = await asyncio.gather(*tasks)

    return [{'country': country['country'],'code': code} for country, code in zip(countries_data, results)]


country_codes = asyncio.run(main(countries_data))
# country_codes = await main(countries_data)

for data, code in zip(countries_data, country_codes):
    data['code'] = code['code']


# Criar DataFrame com os dados coletados
df_countries = pd.DataFrame(countries_data)

print(df_countries)

# conn.close()
