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
from curl_cffi import AsyncSession  # Nome correto da classe



con = sqlite3.connect(r'C:\Users\Prange\Documents\Pessoal\Projetos\Radios Station\tutorial.db')
cur = con.cursor()


# cur.execute("create table countries(contnent,country,url)")


# cur.executemany("INSERT INTO countries VALUES(?, ?, ?)", df_c.values)
# con.commit()
# con.commit()





# df = pd.read_sql_query('select * from countries',con=con,)




# # Coletar url de cada pais

df_c = pd.DataFrame(columns=['Contnent','Country','url'])

url = 'https://instant.audio'

headers = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
}
req = cc_requests.get(url,headers=headers,impersonate="chrome120")
soup = BeautifulSoup(req.content,features='html.parser')

country_section = soup.find('section',{'class':"country-section"})
contnents_box = country_section.find_all('div',{'class':"box active"})



i = 0
for contnent_box in contnents_box:

  contnent_name = contnent_box.strong.a.text
  countries_box = contnent_box.find('ul',{'class':"country-list"})
  countries_el = countries_box.find_all('li')

  for countrie_el in countries_el:

    country_url = countrie_el.a['href']
    country_name = countrie_el.a.text

    df_c.loc[i] = [contnent_name,country_name,country_url]
    i+=1

df_c










# # --------------------------------------------------------------------------------




# Acessar site e coletar estações e codigo do pais 

contnent_name = 'Europe'
country_name = 'Deutschland'

# url = 'https://radiosaovivo.net'
# url = 'https://emisoras.com.gt'
# url = 'https://radioarg.com'
# url = 'https://radio.org.ro/'
url = 'https://radiolisten.de'

headers = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
}
req = cc_requests.get(url,headers=headers,impersonate="chrome120")

soup = BeautifulSoup(req.content,features='html.parser')


# Coletar estações
radios = soup.find('ul',id='radios')
estacoes = radios.find_all('li')[:43]
station_names = [est.a['href'].split('#')[1] for est in estacoes]


# Codigo do pais 
el = soup.find('link',{'as':"fetch",'type':"application/json",'crossorigin':"anonymous"})
country_code = el['href'].split('streams/')[1].split('/')[0]
# country_code


# f'https://api.instant.audio/data/playlist/{country_code}/{station_name}'

urls = [f'https://api.instant.audio/data/playlist/{country_code}/{station_name}' for station_name in station_names]



t1 = time.time()


async def fetch_data(url, session):
    s_name = url.split('/')[-1]
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "*/*",
      "Accept-Encoding": "gzip, deflate, br",
    }
    
    response = await session.get(url, headers=headers)
    print(f"Status Code for {s_name}: {response.status_code}")
    
    if response.status_code == 200:
        return response.json()
    else:
        # print(s_name)
        return None

async def main():
    tdf = pd.DataFrame(columns=['track_artist','track_title','track_image','track_played'])
    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
              

    for result,s_name in zip(results,station_names):
        df = pd.DataFrame(result.get('result',[]))
        df['radio_station'] = s_name
        tdf = pd.concat([tdf,df],axis=0)
    
    tdf['country'] = country_name
    tdf['contnent'] = contnent_name
    return tdf


tdf = asyncio.run(main())

tdf.to_csv(f'{country_name}.csv',index=False)

t2 = time.time()
print(t2-t1)



# df = pd.read_csv(r'C:\Users\Prange\Documents\Pessoal\Projetos\Deutschland.csv')


cur.execute('select artist from tracks').fetchone()

# cur.executemany("INSERT INTO tracks VALUES(?,?,?,?,?,?,?)", df.values)
cur.executemany("INSERT INTO tracks VALUES(?,?,?,?,?,?,?)", tdf.values)
con.commit()



# df = pd.read_csv(r'C:\Users\Prange\Documents\Pessoal\Projetos\România.csv')


# df.to_feather('România.feather')
# df.to_parquet('România.parquet')

# cur.execute('create table tracks(artist,music_title,album_image,time_played,radio_station,country,contnent)')
