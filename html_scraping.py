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
import traceback


class BuscaProduto():
    def __init__(self,texto_pesquisa, max_paginas=2, cidade='', estado='', ordenar_por=''):
        if type(texto_pesquisa) is not list: texto_pesquisa=[texto_pesquisa] # para aceita uma string ou lista de strings
        self.texto_pesquisa = texto_pesquisa
        self.max_paginas = max_paginas
        self.cidade = cidade
        self.estado = estado
        self.ordenar_por = ordenar_por
        self.lista_produtos = []

    def OLX(self, filtrar_titulo = False):
        dict_cidade = {'bh': 'belo-horizonte-e-regiao'}
        dict_ordenar_por = {'data' : '&sf=1'}
        dict_estado = {'bh': 'mg'}
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
        if self.cidade:
            if not self.estado:
                self.estado = dict_estado[self.cidade]
            self.cidade = dict_cidade[self.cidade.lower()]
            self.estado=self.estado.lower()+'.'
        elif self.estado:
            self.estado=self.estado.lower()+'.'
        else:
            self.cidade='brasil'
        if self.ordenar_por:
            self.ordenar_por = dict_ordenar_por[self.ordenar_por]
        for pesquisa in self.texto_pesquisa:
            print(F'******* PESQUISANDO "{pesquisa}"')         
            query_pesquisa = quote(pesquisa)            
            for x in range(1, self.max_paginas+1):
                url = 'https://'+ self.estado + 'olx.com.br/' + self.cidade + '?q=' + query_pesquisa + self.ordenar_por
                if x != 1:
                    url = url+'&o='+str(x)
                print(f'\tPÁGINA {str(x)} - {url}')
                page = requests.get(url, headers=headers)
                # soup = BeautifulSoup(page.content, "html.parser")
                soup = BeautifulSoup(page.content, "lxml")
                produtos = soup.find_all('li', {"class": ["sc-1fcmfeb-2 fvbmlV", "sc-1fcmfeb-2 iezWpY"]})
                # print(produtos)
                print(f'\t\tEncontrou {len(produtos)} resultados')
                if len(produtos)== 0:
                    break
                for produto in produtos:
                    try:
                        # print(produto)
                        titulo_anuncio = produto.findAll("h2")[0].contents[0]
                        titulo_anuncio = re.sub(r'<.*>', '', titulo_anuncio).rstrip()
                        if filtrar_titulo:
                            if not re.match(f'.*{pesquisa.lower()}.*', titulo_anuncio.lower()):
                                continue
                        preco_post = produto.findAll("p", class_="sc-1iuc9a2-8 bTklot sc-ifAKCX eoKYee")[0].contents[0]
                        preco_post = float(preco_post.split()[1].replace('.', ''))
                        data_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[0].contents[0]
                        hora_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[1].contents[0]
                        cidade_bairro = produto.findAll("span", class_="sc-7l84qu-1 ciykCV sc-ifAKCX dpURtf")[0].contents[0]
                        # print(cidade_bairro)
                        if self.cidade != 'brasil':
                            if self.cidade == '':
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
                            estado_post = self.estado[:-1].upper()
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
                        self.lista_produtos.append(dic_produtos)

                    except Exception as e:
                        # print(traceback.format_exc())
                        pass
        return self.lista_produtos
    


if __name__ == '__main__':
    print(f'Iniciando busca...')
    # busca = BuscaProduto('caixa', estado='MG', cidade='BH')
    # busca = BuscaProduto('caixa', estado='mg')
    # busca = BuscaProduto('caixa', cidade='bh')
    busca = BuscaProduto(['sr315a', 'sr315 A'], max_paginas=10)
    # lista_produtos = busca.OLX(filtrar_titulo=False)
    lista_produtos = busca.OLX(filtrar_titulo=True)

    # print(lista_produtos)
    df = pd.DataFrame(data=lista_produtos)
    df = df.drop_duplicates()
    print(df)
    # df.to_excel('dict1.xlsx')
