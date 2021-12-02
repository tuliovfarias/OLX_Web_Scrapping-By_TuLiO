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

lista_produtos = []


def buscarDadosOLX(texto_pesquisa, paginas=2, cidade='', estado='', ordenar_por=''):
    dict_cidade = {'bh': 'belo-horizonte-e-regiao'}
    dict_ordenar_por = {'data' : '&sf=1'}
    dict_estado = {'bh': 'mg'}
    if cidade:
        if not estado:
            estado = dict_estado[cidade]
        cidade = dict_cidade[cidade.lower()]
        estado=estado.lower()+'.'
    elif estado:
        estado=estado.lower()+'.'
    else:
        cidade='brasil'
    if ordenar_por:
        ordenar_por = dict_ordenar_por[ordenar_por]        
    query_pesquisa = quote(texto_pesquisa)
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
    for x in range(1, paginas+1):
        print('PÁGINA ' + str(x))
        url = 'https://'+ estado + 'olx.com.br/' + cidade + '?q=' + query_pesquisa + ordenar_por
        if x != 1:
            url = url+'&o='+str(x)
        print(url)
        page = requests.get(url, headers=headers)
        # soup = BeautifulSoup(page.content, "html.parser")
        soup = BeautifulSoup(page.content, "lxml")
        produtos = soup.find_all('li', {"class": "sc-1fcmfeb-2 fvbmlV"})
        # print(produtos)
        print(f'Encontrou {len(produtos)} resultados')
        if len(produtos)== 0:
            break
        for produto in produtos:
            try:
                titulo_anuncio = produto.findAll("h2")[0].contents[0]
                titulo_anuncio = re.sub(r'<.*>', '', titulo_anuncio)
                preco_post = produto.findAll("p", class_="sc-1iuc9a2-8 bTklot sc-ifAKCX eoKYee")[0].contents[0]
                preco_post = float(preco_post.split()[1].replace('.', ''))
                data_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[0].contents[0]
                hora_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[1].contents[0]
                cidade_bairro = produto.findAll("span", class_="sc-7l84qu-1 ciykCV sc-ifAKCX dpURtf")[0].contents[0]
                # print(cidade_bairro)
                if cidade != 'brasil':
                    if cidade == '':
                        if re.search(r',',cidade_bairro):
                            # cidade_post = cidade_bairro.split(',')[0]
                            # bairro_post=cidade_bairro.split(',')[1].split('-')[0][:-1] # remove espaço no final
                            cidade_post = re.search(r'(.*)(?=,)',cidade_bairro).group()                        
                            bairro_post=re.search(r'(?<=,)(.*)(?= -)',cidade_bairro).group()
                        else:
                            # cidade_post = cidade_bairro.split('-')[0][:-1]
                            cidade_post=re.search(r'(.*)(?= -)',cidade_bairro).group()
                            bairro_post = ''                 
                    else:
                        if re.search(r',',cidade_bairro):
                            # cidade_post = cidade_bairro.split(',')[0]
                            cidade_post = re.search(r'(.*)(?=,)',cidade_bairro).group() 
                            # bairro_post = cidade_bairro.split(',')[1].split('-')[0]
                            bairro_post = re.search(r'(?<=,)(.*)',cidade_bairro).group()
                        else:
                            cidade_post = cidade_bairro
                            bairro_post = ''
                        # print(f'resultado: {cidade_post} {bairro_post}')
                    estado_post = estado[:-1].upper()
                else:
                    cidade_post = re.search(r'(.*)(?= -)',cidade_bairro).group()
                    bairro_post=''
                    estado_post = re.search(r'(?<=-  )(.*)',cidade_bairro).group()
                url_produto = produto.find('a')["href"]

                dic_produtos = {'data': data_post,
                                'hora': hora_post,
                                'titulo': titulo_anuncio,
                                'preco': preco_post,
                                'estado': estado_post,
                                'cidade': cidade_post,
                                'bairro': bairro_post,
                                'url': url_produto, }
                lista_produtos.append(dic_produtos)
            except Exception as e:
                # print(e)
                pass
    # print(titulo_anuncio)

# buscarDadosOLX('caixa', estado='MG', cidade='BH')
# buscarDadosOLX('caixa', estado='mg')
# buscarDadosOLX('caixa', cidade='bh')
buscarDadosOLX('ui24', paginas=10)
# print(lista_produtos)
df = pd.DataFrame(data=lista_produtos)
print(df)
df.to_excel('dict1.xlsx')
