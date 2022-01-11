import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import re
from urllib.parse import quote
import traceback
import os
from multiprocessing import Process, Manager
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import sys
from retry import retry
# from tenacity import retry

class BuscaProduto():
    def __init__(self, produto, texto_pesquisa, ignorar, max_paginas=2, cidade='', estado='', ordenar_por='', paralelizacao=True):
        # if type(texto_pesquisa) is not list: texto_pesquisa=[texto_pesquisa] # para aceitar uma string ou lista de strings
        self.produto = produto
        self.texto_pesquisa = texto_pesquisa.split(',')
        self.ignorar = ignorar.split(',')
        self.max_paginas = max_paginas
        self.cidade = cidade.lower()
        self.estado = estado.lower()
        self.ordenar_por = ordenar_por
        self.lista_produtos = []
        self.paralelizacao=paralelizacao

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
    
    def close(self):
        """synonym for save, to make it more file-like"""
        content = self.save()
        # self.handles.close()
        return content

    def save(self):
        """
        Save workbook to disk.
        """
        pass

    def OLX(self, filtrar_titulo = False):
        self.site='OLX'
        self.url_list=[]
        self.filtrar_titulo=filtrar_titulo

        dict_cidade = {'bh': 'belo-horizonte-e-regiao', 'sp': 'sao-paulo-e-regiao'}
        dict_ordenar_por = {'data' : '&sf=1'}
        dict_estado = {'bh': 'mg', 'sp': 'sp'}
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
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
        print(f"\n******* PESQUISANDO: {self.produto} ({'%s' % ','.join(self.texto_pesquisa)})")
        process_list=[]
        manager = Manager()
        self.lista_produtos = manager.list()
        for pesquisa in self.texto_pesquisa:
            if self.paralelizacao:
                p=Process(target=self._OLX_pesquisa, args=(pesquisa, self.lista_produtos,))
                p.start()
                process_list.append(p)
            else:
                self._OLX_pesquisa(pesquisa, self.lista_produtos)
        if self.paralelizacao:
            for p in process_list: 
                p.join()

        self.lista_produtos = list(self.lista_produtos)

        print(f'---------------> {self.lista_produtos}')

        # print(f'-> Encontrou {len(self.lista_produtos)} resultados')
        # print(f'-> Erro em {count_errors} resultados\n')

        self.df_lista_produtos = pd.DataFrame(data=self.lista_produtos)
        if not self.lista_produtos:
            print("Nenhum resultado encontrado!")
            return self.df_lista_produtos

        self.df_lista_produtos = self.df_lista_produtos.drop_duplicates()
        print(f"{len(self.df_lista_produtos)} resultados sem filtro")

        # try:
        #     locale.setlocale(locale.LC_ALL, 'pt_BR')
        # except:
        #    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8') #sudo dpkg-reconfigure locales
        # today=date.today()
        # self.df_lista_produtos.data.replace({'Hoje': today.strftime('%d %b'), 'Ontem': (today - timedelta(1)).strftime('%d %b')}, inplace=True)

        # for i, row in self.df_lista_produtos.iterrows():
        #     self.df_lista_produtos.data[i] = datetime.strptime(self.df_lista_produtos.data[i], '%d %b').date().strftime('2021-%m-%d')
        #     self.df_lista_produtos.hora[i] = datetime.strptime(self.df_lista_produtos.hora[i], '%H:%M').time().strftime('%H:%M')

        # self.df_lista_produtos.data = [datetime.strptime(x, '%d %b').date().strftime(f'{today.year}-%m-%d') for x in self.df_lista_produtos.data]
        # self.df_lista_produtos.hora = [datetime.strptime(x, '%H:%M').time().strftime('%H:%M') for x in self.df_lista_produtos.hora]

        # self.df_lista_produtos["data_hora"] = self.df_lista_produtos["data"] +' '+ self.df_lista_produtos["hora"] # self.df_lista_produtos.apply(lambda r : pd.datetime.combine(r['data'],r['hora']),1)
        self.df_lista_produtos["data_hora"] = self.df_lista_produtos["data_hora"]
        # self.df_lista_produtos.drop(columns=['data', 'hora'], axis=1, inplace=True)
        self.df_lista_produtos = self.df_lista_produtos[['data_hora', 'titulo', 'preco', 'estado', 'cidade', 'bairro', 'url']]
                
        self.df_lista_produtos.data_hora = self.df_lista_produtos.data_hora.apply(pd.to_datetime)

        self.df_lista_produtos.sort_values(by=['data_hora'], inplace=True, ascending=False)
        self.df_lista_produtos = self.df_lista_produtos.reset_index(drop=True)

        return self.df_lista_produtos
    
    def _OLX_pesquisa(self,pesquisa, lista_produtos):
        count_errors=0
        pesquisa = pesquisa.lstrip().rstrip() # remove espaço antes e depois
        print(f'\t- {pesquisa}:')        
        query_pesquisa = quote(pesquisa)            
        for pagina in range(1, self.max_paginas+1):
            url = 'https://'+ self.estado + 'olx.com.br/' + self.cidade + '?q=' + query_pesquisa + self.ordenar_por
            if pagina != 1:
                url = url+'&o='+str(pagina)
            else:
                self.url_list.append(url)

            page = requests.get(url, headers=self.headers)

            soup = BeautifulSoup(page.content, "html.parser")
            # produtos = soup.find_all('li', {"class": ["sc-1fcmfeb-2 fvbmlV", "sc-1fcmfeb-2 iezWpY", "sc-1fcmfeb-2 bTBcfv"]})
            # produtos = soup.find_all('div', {"class": ["fnmrjs-1 gIEtsI"]})

            produtos = soup.find('ul', {"id": ["ad-list"]})
            if produtos == None or len(produtos)== 0:
                if pagina == 1:
                    print(f'\t\tNenhum resultado')
                break
            produtos = produtos.findChildren('li',recursive=False)
            
            print(f'\t\tPÁGINA {str(pagina)} - {url}')
            for produto in produtos:
                try:
                    url_produto = produto.find("a")["href"]
                    page_produto = requests.get(url_produto, headers=self.headers)
                    soup = BeautifulSoup(page_produto.content, "html.parser")

                    data = soup.find('script', {"id": "initial-data"})
                    search = re.search('"listTime":"(.*?).000Z"',str(data)).group(1)
                    data_hora_post = datetime.fromisoformat(search) - timedelta(hours=3)

                    titulo_anuncio = produto.findAll("h2")[0].contents[0]
                    # print(f'*********************************{titulo_anuncio}')
                    titulo_anuncio = re.sub(r'<.*>', '', titulo_anuncio).rstrip()
                    continue_flag = False
                    if self.filtrar_titulo:
                        if not re.match(f'.*{pesquisa.lower()}.*', titulo_anuncio.lower()):
                            continue
                        if self.ignorar[0] != '':
                            for ignora in self.ignorar:
                                if re.match(f'{ignora.lower()}', titulo_anuncio.lower()):
                                    continue_flag = True
                            if continue_flag: continue
                    preco_post = produto.findAll("p", class_="sc-1iuc9a2-8 bTklot sc-ifAKCX eoKYee")[0].contents[0]
                    preco_post = float(preco_post.split()[1].replace('.', ''))
                    # data_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[0].contents[0]
                    # hora_post = produto.findAll("span", class_="wlwg1t-1 fsgKJO sc-ifAKCX eLPYJb")[1].contents[0]
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

                    dic_produtos = {'data_hora': data_hora_post,
                                    'titulo': titulo_anuncio,
                                    'preco': preco_post,
                                    'estado': estado_post,
                                    'cidade': cidade_post,
                                    'bairro': bairro_post,
                                    'url': url_produto, }
                    lista_produtos.append(dic_produtos)
                except IndexError:
                    # print(traceback.format_exc())
                    count_errors = count_errors+1
                except Exception as e:
                    # print(traceback.format_exc())
                    # print(e)
                    pass
                self.lista_produtos = lista_produtos

    def Filtrar(self, preco_max=None, intervalo=None, email = False, json_cred_path=None):
        dias, horas, minutos = 0,0,0
        self.preco_max = preco_max
        self.intervalo = intervalo
        # today = pd.to_datetime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
        today = datetime.now()
        # print(f'----------today={today}')

        if self.lista_produtos:
            if preco_max:
                print(f'Filtrando preços menores que: {preco_max}')
                self.df_lista_produtos = self.df_lista_produtos.query(f'preco <= {preco_max}').reset_index(drop=True)
                print(self.df_lista_produtos)
            if intervalo:
                tipos={'d': 'dias', 'h' : 'horas', 'm' : 'minutos'}
                tipo = intervalo[-1]
                intervalo = int(intervalo[:-1])
                if tipo == 'd': dias = intervalo
                elif tipo == 'h': horas = intervalo
                elif tipo == 'm': minutos = intervalo
                self.intervalo_tipo = f'{self.intervalo[:-1]} {tipos[tipo]}'

                self.intervalo_td = timedelta(days=dias, hours=horas, minutes=minutos)
                print(f"Filtrando datas maiores que: {(today-self.intervalo_td).strftime('%d/%m/%Y %H:%M:%S')}")
                time_in_seconds = (self.df_lista_produtos['data_hora'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') + 60*60*3 # GMT -3:00h
                
                # self.df_lista_produtos=self.df_lista_produtos.loc[self.df_lista_produtos['data_hora'].dt.date >= (today-timedelta(intervalo)).date()]
                self.df_lista_produtos=self.df_lista_produtos.loc[time_in_seconds >= int((today-self.intervalo_td).timestamp())]

                # self.df_lista_produtos = self.df_lista_produtos.query(f'data_hora >= {(today-timedelta(intervalo)).date()}').reset_index(drop=True)
            if not self.df_lista_produtos.empty and email:
                self.EnviarEmail(json_cred_path, email)
            return self.df_lista_produtos
        else:
            return self.df_lista_produtos

    #@staticmethod
    def EnviarEmail(self, json_cred_path, email_para):
        with open(json_cred_path) as cred:
            dados = json.load(cred)
            host = dados["e-mail"]["host"]
            port = dados["e-mail"]["port"]
            user = dados["e-mail"]["user"]
            password = dados["e-mail"]["password"]
            email_de = dados["e-mail"]["from"]
            # email_para = dados["e-mail"]["to"]
        server = smtplib.SMTP_SSL(host, port)
        server.login(user, password)
        email_msg = MIMEMultipart()
        email_msg['From'] = email_de
        email_msg['To'] = email_para
        email_msg['Subject'] = f"Busca em {self.site} - {self.produto}"
        # email_msg['Subject'] = f"Busca em {self.site} {'(%s)' % ', '.join(self.texto_pesquisa)}"
        html = f"""\
                <html>
                <head></head>
                <body>
                    <p>URLs buscadas:<br>{'<br>'.join(self.url_list)}</p>
                    <p>Resultados:</p>
                """
        if self.preco_max:
            html = html + f"<p>- preço abaixo de: R$ {self.preco_max},00</p>"
        if self.intervalo_td:
            html = html + f"<p>- publicado até: {self.intervalo_tipo} atrás</p>"
        html = html + f"""\
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
        
def get_dict_from_xls(xls_path):
    df = pd.read_excel(xls_path, sheet_name='Filtros busca', engine='openpyxl', usecols='A:K').dropna(how='all')
    df = df.fillna('') 
    # df = df.replace('\xa0', ' ')
    return df

# @retry(tries=5, delay=60)
def busca_produto(json_cred_path, dado, paralelizacao):
    with BuscaProduto(dado['produto'],dado['filtros'],dado['ignorar'],int(dado['max_paginas']),dado['cidade'],dado['estado'],dado['ordenar_por'],paralelizacao) as busca:
        lista_produtos = busca.OLX(dado['filtrar_titulo'])
        # print(lista_produtos)
        lista_filtrada = busca.Filtrar(dado['preco_max'],dado['intervalo'],dado['email'],json_cred_path)
    if not lista_filtrada.empty:
        print(lista_filtrada)
    else:
        print('Nenhum resultado no filtro!')

def main():
    source_dir = os.path.dirname(__file__)
    search_filters_path = os.path.join(source_dir,'busca.xlsx')
    busca_dict_list = get_dict_from_xls(search_filters_path)
    json_cred_path = os.path.join(source_dir,'cred.json')
    procs_list=[]
    print(f'Iniciando busca...')
    # outputs = pool.map(square, inputs)
    # print(busca_dict_list)
    for dado in busca_dict_list.to_dict(orient="records"):
        print(dado)
        if paralelizacao:    
            p = Process(target=busca_produto, args=[json_cred_path, dado, paralelizacao])
            p.start()
            procs_list.append(p)
        else:
            busca_produto(json_cred_path, dado, paralelizacao)
        # p.join()
        # subprocess.Popen(busca_produto(dado))
        print(100*"-")
        # with pd.ExcelWriter(search_filters_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        # lista_produtos.to_excel(writer, sheet_name='Resultados', index=False)
    if paralelizacao: 
        for p in procs_list:
            p.join()
    print("Finalizou todas as pesquisas!")


if __name__ == '__main__':
    paralelizacao=True
    try:
        main()
    except:
        print(traceback.format_exc())
