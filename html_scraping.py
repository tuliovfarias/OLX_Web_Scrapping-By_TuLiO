import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import json
from difflib import SequenceMatcher
from selenium import webdriver
import time
from datetime import date
import re
from urllib.parse import quote

listJson = []


def buscarDadosOLX(texto_pesquisa, pages=2, regiao='BH', ordenar_por=''):
    regioes = {'BH': 'belo-horizonte-e-regiao'}
    prefix = {'BH': 'mg'}
    query_pesquisa = quote(texto_pesquisa)
    if ordenar_por == 'data':
        ordenar_por = '&sf=1'
    for x in range(1, pages+1):
        print('LOOP NÚMERO' + str(x))
        url = 'https://'+prefix[regiao]+'.olx.com.br/' + \
            regioes[regiao]+'?q='+query_pesquisa+ordenar_por
        if x == 1:
            print('primeira pagina')
        else:
            url = url+'&o='+str(x)
        print(url)
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
    page = requests.get(url, headers=headers)
    # soup = BeautifulSoup(page.content, "html.parser")
    soup = BeautifulSoup(page.content, "lxml")
    itens = soup.find_all('li', {"class": "sc-1fcmfeb-2 fvbmlV"})
    # print(resultado)
    print(f'Encontrou {len(itens)} resultados')
    for item in itens:
        try:
            titulo_anuncio = item.findAll("h2")[0].contents[0]
            preco = item.findAll(
                "p", class_="sc-1iuc9a2-8 bTklot sc-ifAKCX eoKYee")[0].contents[0]
            preco = float(preco.split()[1].replace('.', ''))
            data_post = item.findAll(
                "span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[0].contents[0]
            hora_post = item.findAll(
                "span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[1].contents[0]
            cidade_bairro = item.findAll(
                "span", class_="sc-7l84qu-1 ciykCV sc-ifAKCX dpURtf")[0].contents[0]
            url_produto = item.find('a')["href"]

            json = {'titulo_anuncio': titulo_anuncio,
                    'preco': preco,
                    'cidade_bairro': cidade_bairro,
                    'url_produto': url_produto,
                    'data_post': data_post,
                    'hora_post': hora_post, }
            listJson.append(json)
        except:
            pass
    # print(titulo_anuncio)


buscarDadosOLX('caixa de som')
print(listJson)
