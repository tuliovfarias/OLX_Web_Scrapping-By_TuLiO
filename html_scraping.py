import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import re
from urllib.parse import quote
import locale
# import time
import traceback

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json

class BuscaProduto():
    def __init__(self,texto_pesquisa, max_paginas=2, cidade='', estado='', ordenar_por=''):
        if type(texto_pesquisa) is not list: texto_pesquisa=[texto_pesquisa] # para aceitar uma string ou lista de strings
        self.texto_pesquisa = texto_pesquisa
        self.max_paginas = max_paginas
        self.cidade = cidade.lower()
        self.estado = estado.lower()
        self.ordenar_por = ordenar_por
        self.lista_produtos = []

    def OLX(self, filtrar_titulo = False):
        self.site='OLX'
        self.url_list=[]

        count_errors=0
        dict_cidade = {'bh': 'belo-horizonte-e-regiao', 'sp': 'sao-paulo-e-regiao'}
        dict_ordenar_por = {'data' : '&sf=1'}
        dict_estado = {'bh': 'mg', 'sp': 'sp'}
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
        if self.cidade:
            if not self.estado:
                self.estado = dict_estado[self.cidade]
            self.cidade = dict_cidade[self.cidade]
            self.estado=self.estado+'.'
        elif self.estado:
            self.estado=self.estado+'.'
        else:
            self.cidade='brasil'
        if self.ordenar_por:
            self.ordenar_por = dict_ordenar_por[self.ordenar_por]
        for pesquisa in self.texto_pesquisa:
            print(f'\n******* PESQUISANDO "{pesquisa}"')         
            query_pesquisa = quote(pesquisa)            
            for pagina in range(1, self.max_paginas+1):
                url = 'https://'+ self.estado + 'olx.com.br/' + self.cidade + '?q=' + query_pesquisa + self.ordenar_por
                if pagina != 1:
                    url = url+'&o='+str(pagina)
                else:
                    self.url_list.append(url)
                page = requests.get(url, headers=headers)
                # soup = BeautifulSoup(page.content, "html.parser")
                soup = BeautifulSoup(page.content, "lxml")
                produtos = soup.find_all('li', {"class": ["sc-1fcmfeb-2 fvbmlV", "sc-1fcmfeb-2 iezWpY"]})
                # print(produtos)
                if len(produtos)== 0:
                    break
                print(f'\tPÁGINA {str(pagina)} - {url}')
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
                        if self.cidade != 'brasil':
                            if self.cidade == '':
                                if re.search(r',',cidade_bairro):
                                    cidade_post = re.search(r'(.*)(?=,)',cidade_bairro).group() # cidade_post = cidade_bairro.split(',')[0]                       
                                    bairro_post=re.search(r'(?<=,)(.*)(?= -)',cidade_bairro).group() # bairro_post=cidade_bairro.split(',')[1].split('-')[0][:-1] # remove espaço no final
                                else:
                                    cidade_post=re.search(r'(.*)(?= -)',cidade_bairro).group()
                                    bairro_post = ''                 
                            else:
                                if re.search(r',',cidade_bairro):
                                    cidade_post = re.search(r'(.*)(?=,)',cidade_bairro).group() 
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
                    except IndexError:
                        # print(traceback.format_exc())
                        count_errors = count_errors+1
        # print(f'-> Encontrou {len(self.lista_produtos)} resultados')
        # print(f'-> Erro em {count_errors} resultados\n')

        self.df_lista_produtos = pd.DataFrame(data=self.lista_produtos)
        if not self.lista_produtos:
            print("Nenhum resultado encontrado!")
            return self.df_lista_produtos

        self.df_lista_produtos = self.df_lista_produtos.drop_duplicates()
        print(f"{len(self.df_lista_produtos)} resultados sem filtro")

        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR')
        except:
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8') #sudo dpkg-reconfigure locales
        today=date.today()
        self.df_lista_produtos.data.replace({'Hoje': today.strftime('%d %b'), 'Ontem': (today - timedelta(1)).strftime('%d %b')}, inplace=True)
        # for i, row in self.df_lista_produtos.iterrows():
        #     self.df_lista_produtos.data[i] = datetime.strptime(self.df_lista_produtos.data[i], '%d %b').date().strftime('2021-%m-%d')
        #     self.df_lista_produtos.hora[i] = datetime.strptime(self.df_lista_produtos.hora[i], '%H:%M').time().strftime('%H:%M')
        self.df_lista_produtos.data = [datetime.strptime(x, '%d %b').date().strftime(f'{today.year}-%m-%d') for x in self.df_lista_produtos.data]
        self.df_lista_produtos.hora = [datetime.strptime(x, '%H:%M').time().strftime('%H:%M') for x in self.df_lista_produtos.hora]

        self.df_lista_produtos["data_hora"] = self.df_lista_produtos["data"] +' '+ self.df_lista_produtos["hora"]
        self.df_lista_produtos.drop(columns=['data', 'hora'], axis=1, inplace=True)
        self.df_lista_produtos = self.df_lista_produtos[['data_hora', 'titulo', 'preco', 'estado', 'cidade', 'bairro', 'url']]
        # self.df_lista_produtos.apply(lambda r : pd.datetime.combine(r['data'],r['hora']),1)
        
        self.df_lista_produtos.data_hora = self.df_lista_produtos.data_hora.apply(pd.to_datetime)
        # print(f'--------------{type(self.df_lista_produtos.data_hora[0])}')

        # self.df_lista_produtos.data_hora = self.df_lista_produtos.data_hora.apply(pd.to_datetime(self.df_lista_produtos.data_hora,format='%d%b%Y:%H:%M:%S.%f'))
        # self.df_lista_produtos.data_hora = self.df_lista_produtos.data_hora.apply(lambda x: datetime.date(x.year,x.month,x.day))

        self.df_lista_produtos.sort_values(by=['data_hora'], inplace=True, ascending=False)
        self.df_lista_produtos = self.df_lista_produtos.reset_index(drop=True)

        return self.df_lista_produtos

    def FiltrarPreco(self, preco_max=None, dias=None, email = False):
        self.preco_max = preco_max
        self.dias = dias
        # today = pd.to_datetime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
        today = datetime.now()
        # print(f'----------today={today}')

        if self.lista_produtos:
            if preco_max:
                print(f'Filtrando preços menores que: {preco_max}')
                self.df_lista_produtos = self.df_lista_produtos.query(f'preco <= {preco_max}').reset_index(drop=True)
            if dias:
                print(f"Filtrando datas maiores que: {(today-timedelta(dias)).date()}")
                self.df_lista_produtos=self.df_lista_produtos.loc[self.df_lista_produtos['data_hora'].dt.date >= (today-timedelta(dias)).date()]
                # self.df_lista_produtos = self.df_lista_produtos.query(f'data_hora >= {(today-timedelta(dias)).date()}').reset_index(drop=True)
            if not self.df_lista_produtos.empty and email:
                self.EnviarEmail()
            return self.df_lista_produtos
        else:
            return self.df_lista_produtos

    #@staticmethod
    def EnviarEmail(self):
        with open('cred.json') as cred:
            dados = json.load(cred)
            host = dados["e-mail"]["host"]
            port = dados["e-mail"]["port"]
            user = dados["e-mail"]["user"]
            password = dados["e-mail"]["password"]
            email_de = dados["e-mail"]["from"]
            email_para = dados["e-mail"]["to"]
        server = smtplib.SMTP_SSL(host, port)
        server.login(user, password)
        email_msg = MIMEMultipart()
        email_msg['From'] = email_de
        email_msg['To'] = email_para
        email_msg['Subject'] = f"Busca em {self.site} {'(%s)' % ', '.join(self.texto_pesquisa)}"
        html = f"""\
                <html>
                <head></head>
                <body>
                    <p>URLs buscadas:<br>{'<br>'.join(self.url_list)}</p>
                    <p>Mostrando produtos abaixo de R$ {self.preco_max},00 de até {self.dias} dia(s) atrás</p>
                    <p>{self.df_lista_produtos.to_html(index=False)}</p>
                </body>
                </html>
                """
        email_msg.attach(MIMEText(html, 'html'))

        server.sendmail(email_de,
                        email_para,
                        email_msg.as_string(),
                        )
        print(f'E-mail enviado para: {email_para}')
        server.quit()
        

if __name__ == '__main__':
    print(f'Iniciando busca...')
    with open('search.json') as search:
        dados = json.load(search)["search"]
    for dado in dados:
        # print(dado)
        busca = BuscaProduto(dado['texto'],dado['max_paginas'],dado['cidade'],dado['estado'],dado['ordenar_por'])
        lista_produtos = busca.OLX(dado['filtrar_titulo'])
        # print(lista_produtos)
        filtro_preco = busca.FiltrarPreco(dado['preco_max'],dado['dias'],dado['email'])
        if not filtro_preco.empty:
            print(filtro_preco)
        else:
            print('Nenhum resultado no filtro!')

    # lista_produtos.to_excel('dict1.xlsx')