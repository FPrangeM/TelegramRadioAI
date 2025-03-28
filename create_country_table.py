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
conn = sqlite3.connect("tutorial.db")
cursor = conn.cursor()

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



# Criar DataFrame com os dados coletados
df_countries = pd.DataFrame(countries_data)

conn.close()