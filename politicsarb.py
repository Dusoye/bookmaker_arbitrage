import requests
from datetime import date
from bs4 import BeautifulSoup as bs
import pandas as pd
import xmltodict

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36"}
url = "https://www.oddschecker.com/sport/politics/sitemap.xml"

sitemap = requests.get(url, headers = headers)
raw = xmltodict.parse(sitemap.text)

data = [[r["loc"], r["changefreq"]] for r in raw["urlset"]["url"]]
sitelist = pd.DataFrame(data, columns=["links", "lastmod"])

for link in sitelist['links']:
    page = requests.get(link, headers = headers)
    soup = bs(page.content, 'html.parser')
    for tr in soup.findAll('tbody')[0].findAll('tr'):
        print(tr)
    

