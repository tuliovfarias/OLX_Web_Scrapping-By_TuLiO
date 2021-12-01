from lxml import html
import requests
from bs4 import BeautifulSoup

# URL=f'https://realpython.com/'
URL=f'https://mg.olx.com.br/belo-horizonte-e-regiao?q=caixa%20de%20som&sf=1'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}

page = requests.get(URL, headers=headers)
soup = BeautifulSoup(page.content, "html.parser")
# print(soup)
results = soup.find('script',id="initial-data",string="R$")
print(results.prettify())

# print(page.text)
# tree = html.fromstring(page.content)
# print(tree)