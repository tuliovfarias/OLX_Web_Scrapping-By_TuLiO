import subprocess
import sys
import os

def install_requirements(requirements_file):
    subprocess.check_output([sys.executable, "-m", "pip", "install", "-r", requirements_file])
requirements_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),"requirements.txt")
install_requirements(requirements_file)

import asyncio
import locale
from multiprocessing import Process
import shutil
import pandas as pd
# import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import re
from urllib.parse import quote
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from retry import retry
import logging
from typing import List, Dict, Tuple, Any
import json
import time
import concurrent.futures   
from contexttimer import Timer  
from config import CRED_FILE, LOOP_SECONDS, BUSCA_FILE, GSHEET_CRED_FILE, ORIGEM_DEFAULT, PARALELIZAR
import cloudscraper

from gsheet_API import GSheetAPI

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
requests = cloudscraper.create_scraper()

class BuscaProduto():

    def __init__(self, **kwargs:str):
        self.produto = kwargs['produto']
        self.texto_pesquisa = kwargs['filtros'].split(',')
        self.ignorar = kwargs['ignorar'].split(',')
        self.max_paginas = int(kwargs['max_paginas'])
        self.cidade = kwargs['cidade'].lower()
        self.estado = kwargs['estado'].lower()
        self.ordenar_por = kwargs['ordenar_por']
       
        self.lista_produtos = []
        self.filtrar_titulo = True

        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
    
    def close(self):
        content = self.save()
        return content

    def save(self):
        pass

    def OLX(self):
        self.site='OLX'

        # criar parametro para regiao ou bairro
        dict_cidade = {'bh': 'belo-horizonte-e-regiao', 'sp': 'sao-paulo-e-regiao', 'rj' : 'rio-de-janeiro-e-regiao'}
        dict_ordenar_por = {'data' : '&sf=1'}
        dict_estado = {'bh': 'mg', 'sp': 'sp', 'rj' : 'rj'}

        if self.cidade:
            if not self.estado:
                self.estado = dict_estado[self.cidade]
            self.cidade = dict_cidade[self.cidade]
            self.estado = self.estado+'.'
        elif self.estado:
            self.estado = self.estado+'.'
        else:
            self.cidade = 'brasil'
        if self.ordenar_por:
            self.ordenar_por = dict_ordenar_por[self.ordenar_por]

        logging.info(f"******* PESQUISANDO: {self.produto} ({'%s' % ','.join(self.texto_pesquisa)})")
        run_func_in_parallel_ThreadPool(self._OLX_pesquisa, self.texto_pesquisa)
        # for pesquisa in self.texto_pesquisa:  # Executa não paralelizado
        #    self._OLX_pesquisa(pesquisa)
        self.lista_produtos = list(self.lista_produtos)

        self.df_lista_produtos = pd.DataFrame(data=self.lista_produtos)
        
        if not self.lista_produtos:
            logging.info("Nenhum resultado encontrado!")
            return self.df_lista_produtos

        self.df_lista_produtos.drop_duplicates(inplace=True)
        self.df_lista_produtos['data_hora'] = self.df_lista_produtos['data_hora'].apply(pd.to_datetime)
        self.df_lista_produtos.sort_values(by=['data_hora'], inplace=True, ascending=False)
        self.df_lista_produtos = self.df_lista_produtos.reset_index(drop=True)

        logging.info(f"{len(self.df_lista_produtos)} resultados sem filtro")

        return self.df_lista_produtos
    
    def _OLX_pesquisa(self, pesquisa:str):
        page_found_flag = False
        pesquisa = pesquisa.lstrip().rstrip() # remove espaço antes e depois
        for pagina in range(1, self.max_paginas+1):
            url = self.criar_url_base(pesquisa)
            page_found_flag = self.OLX_pesquisa_pagina(url, pagina, pesquisa)
            if not page_found_flag: break
        
    def criar_url_base(self, pesquisa:str) -> str:
        url = 'https://www.olx.com.br/estado-' + self.estado.replace(".", "") + "/" + self.cidade + '?q=' + quote(pesquisa) + self.ordenar_por
        return url
           
    def OLX_pesquisa_pagina(self, url:str, pagina:int, pesquisa:str):
        url = url+'&o='+str(pagina)

        logging.info(f'\tPÁGINA {str(pagina)} ({pesquisa}) - {url}')
        # page = requests.get(url, headers=self.headers)
        page = requests.get(url)
        if re.search("Sorry, you have been blocked",page.text):
            logging.error(f"Page {url} returned: Sorry, you have been blocked")
            sys.exit()
        soup = BeautifulSoup(page.content, "lxml") #"html.parser"
        produtos = soup.find('main', {"id": ["main-content"]})

        if produtos == None or len(produtos)== 0:
            logging.debug(f'\t\t\tNenhum resultado na página {str(pagina)}')
            return False

        produtos = produtos.findAll('div', recursive=False)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for produto in produtos:
                produto:BeautifulSoup

                try:
                    produto = produto.next_element;                    
                    url_produto = produto.find("a")["href"]
                except TypeError:
                    logging.debug(f"URL not found in: \n{produto}\n\n")
                    continue

                try:
                    titulo_anuncio = produto.findAll("h2")[0].contents[0]
                    titulo_anuncio = re.sub(r'<.*>', '', titulo_anuncio).rstrip()
                    logging.debug(f'Título: {titulo_anuncio}')
                    if self.filtrar_titulo:
                        if not self.comparar_regex_search_permut(pesquisa, titulo_anuncio):
                            continue
                    if self.ignorar != ['']:
                        for ignora in self.ignorar:
                            if titulo_anuncio.lower().find(ignora.lower()):
                                continue      
                except (TypeError, IndexError):
                    logging.debug(f"Título não encontrado na página: \n{produto}\n\n")

                executor.submit(self.append_prod_list, url_produto) # Executa paralelizado
                # self.append_prod_list(url_produto) # Executa não paralelizado
        return True

    @staticmethod
    def comparar_regex_search(str1:str, str2:str) -> bool:
        if re.match(f'.*{str1.lower()}.*', str2.lower()):
            return True
        else:
            return False

    @staticmethod
    def comparar_regex_search_permut(str1:str, str2:str) -> bool:
        '''
        Retorna True caso str 2 conter todas as palavras de str1 e False, caso contrário.
        Caso str1 tiver mais de uma palavra faz todas as permutações
        e considera que pode ter qualquer outra coisa entre as palavras.
        Não é case sensitive
        '''
        str1_fmt = str1.lower()
        str2_fmt = str2.lower()
        if re.match(f'.*{str1_fmt}.*', str2_fmt):
            return True
        elif str1_fmt.find(" "): 
            from itertools import permutations
            str1_palavras = str1_fmt.split()
            for perm in permutations(str1_palavras, len(str1_palavras)):
                perm_regex = f"'.*{'.*'.join(perm)}.*'"
                perm_regex = '.*'.join(perm)
                logging.debug(f"{perm_regex} -> {str2_fmt} - {re.search(perm_regex, str(str2_fmt))}")
                if re.search(perm_regex, str2_fmt):
                    logging.debug(f"Título confere: {perm_regex} -> {str2_fmt}")                    
                    return True
        logging.debug(f"Título não confere: {perm_regex} -> {str2_fmt}")                    
        return False
    
    def append_prod_list(self, url_produto:str):
        self.lista_produtos.append(self.OLX_pesquisa_prod(url_produto))

    @staticmethod
    def OLX_pesquisa_prod(url_produto:str):
        try:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
            # page_produto = requests.get(url_produto, headers=headers)
            page_produto = requests.get(url_produto)
            soup_produto = BeautifulSoup(page_produto.content, "lxml") # features="lxml"

            dados_str = str(soup_produto.find('script', {"id": "initial-data"}))
            if dados_str.find('&quot;'):
                dados_str = dados_str.replace('&quot;','"')
            datetime_str = re.search(r'(?<="listTime":").*?(?=.000Z")',dados_str)

            if datetime_str:                
                data_hora_post = datetime.fromisoformat(datetime_str.group()) - timedelta(hours=3)
            if not datetime_str:
                datetime_str = re.search(r'(?<=origListTime":).*?(?=,)', dados_str)
                if datetime_str:
                    data_hora_post = datetime.fromtimestamp(int(datetime_str.group())) - timedelta(hours=3)
                else:
                    logging.error(f"Não encontrou a data de publicação do produto: {url_produto} - {dados_str}")
                    return
            logging.debug(f'Data/hora de publicação: {data_hora_post}')

            regex_list = [
                r'(?<=\"subject\":\").*?(?=\")',
                r'(?<=\"price\":\"R\$ ).*?(?=\")',
                r'(?<=\"Município\",\"value\":\").*?(?=\")',
                r'(?<=\"Bairro\",\"value\":\").*?(?=\")'
            ]
            async def search_and_extract(pattern:re.Pattern, string:str):
                match = re.search(pattern, string)
                return match.group() if match else ''
            async def run_searchs():
                tasks = [asyncio.create_task(search_and_extract(regex, dados_str)) for regex in regex_list]
                return await asyncio.gather(*tasks)                
            titulo_anuncio, preco_post, cidade_post, bairro_post = asyncio.run(run_searchs())

            preco_post = float(preco_post.replace('.','')) if preco_post else 0.0
            estado_post = url_produto[8:10].upper()

            dic_produtos = {'data_hora': data_hora_post,
                            'titulo': titulo_anuncio,
                            'preco': preco_post,
                            'estado': estado_post,
                            'cidade': cidade_post,
                            'bairro': bairro_post,
                            'url': url_produto, }
            logging.debug(dic_produtos)
            # self.lista_produtos.append(dic_produtos)
            return dic_produtos

        except Exception:
            logging.error(traceback.format_exc())
            # count_errors = count_errors+1

    def filtrar(self, preco_max:float=None, intervalo=None):
        self.preco_max = preco_max
        today = datetime.now()
        if self.lista_produtos:
            if preco_max:
                logging.info(f'Filtrando preços menores que: {locale.currency(preco_max, grouping=True)}')
                # self.df_lista_produtos = [self.df_lista_produtos['preco'] <= preco_max]
                self.df_lista_produtos.query(f'preco <= {preco_max}', inplace=True)
            if intervalo:
                self.intervalo_timedelta, self.intervalo_tipo_str = self.format_intervalo(intervalo)
                logging.info(f"Filtrando datas maiores que: {(today-self.intervalo_timedelta).strftime('%d/%m/%Y %H:%M:%S')} ({self.intervalo_tipo_str} atrás)")
                time_in_seconds = (self.df_lista_produtos['data_hora'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') + 60*60*3 # GMT -3:00h
                self.df_lista_produtos = self.df_lista_produtos.loc[time_in_seconds >= int((today-self.intervalo_timedelta).timestamp())]
            
            self.df_lista_produtos.reset_index(drop=True, inplace=True)
        return self.df_lista_produtos

    @staticmethod
    def format_intervalo(intervalo:str):
        dias, horas, minutos = 0,0,0
        tipos={'d': 'dias', 'h' : 'horas', 'm' : 'minutos'}
        tipo = intervalo[-1]
        intervalo_num = int(intervalo[:-1])
        if tipo == 'd': dias = intervalo_num
        elif tipo == 'h': horas = intervalo_num
        elif tipo == 'm': minutos = intervalo_num
        intervalo_timedelta = timedelta(days=dias, hours=horas, minutes=minutos)
        intervalo_tipo_str = f'{intervalo[:-1]} {tipos[tipo]}'        
        return intervalo_timedelta, intervalo_tipo_str

    @retry(tries=3, delay=10)
    def EnviarEmail(self, json_cred_path, email_para : str = None):
        with open(json_cred_path) as cred:
            dados = json.load(cred)
            host = dados["e-mail"]["host"]
            port = dados["e-mail"]["port"]
            user = dados["e-mail"]["user"]
            password = dados["e-mail"]["password"]
            email_de = dados["e-mail"]["from"]
            ssl = dados["e-mail"]["ssl"]
            if not email_para: 
                email_para = email_de
        if ssl:
            server = smtplib.SMTP_SSL(host, port) 
        else: 
            server = smtplib.SMTP(host, port)
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
                    <p>Resultados:</p>
                """
        if self.preco_max:
            html = html + f"<p>- preço abaixo de: {locale.currency(self.preco_max, grouping=True)}</p>"
        if self.intervalo_timedelta:
            html = html + f"<p>- publicado até: {self.intervalo_tipo_str} atrás</p>"
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
        logging.info(f'E-mail enviado para: {email_para}')
        server.quit()
        
def get_dict_from_xls(xls_path) -> List[Dict[str,str]]:
    df = pd.read_excel(xls_path, sheet_name='Filtros busca', engine='openpyxl', usecols='A:K').dropna(how='all')
    df = df.fillna('') 
    # df = df.replace('\xa0', ' ')
    return list(df.to_dict(orient = "records"))

def busca_produto_e_envia_email(dado:Dict[str,Any]) -> Tuple[str, pd.DataFrame]:
    busca = BuscaProduto(**dado)
    lista_produtos = busca.OLX()
    # logging.info(lista_produtos.to_dict(orient = "records"))
    preco_max = str(dado['preco_max']).replace('.','')
    preco_max = float(preco_max) if preco_max != '' else None
    df_lista_filtrada = busca.filtrar(preco_max = preco_max, intervalo = dado['intervalo'])
    if not df_lista_filtrada.empty:
        logging.info(f"{len(df_lista_filtrada)} resultados com filtro:\n{df_lista_filtrada}")
        busca.EnviarEmail(os.path.join(os.path.dirname(__file__),'cred.json'), email_para = dado['email'])
    else:
        logging.info('Nenhum resultado no filtro!')
        pass
    return dado['produto'], df_lista_filtrada

def run_func_in_parallel_Process(fn, args):
    processes_list = []
    for arg in args:
        p = Process(target=fn, args=(arg,))
        p.start()
        processes_list.append(p)                  
    for p in processes_list: 
        p : Process
        p.join()

def run_func_in_parallel_ThreadPool(fn, args_dict:dict):
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(fn, args_dict))

def get_sheet_id_from_json(json_cred_path):
    with open(json_cred_path) as cred:
        dados = json.load(cred)
        return dados["google-sheet"]["sheet_id"]

def get_dict_from_gsheet(sheet_id:str, gsheet_cred_path:str, sheet_range) -> List[Dict[str,str]]:
    '''
    Get spreadsheet data with the id "sheet_id" in the sheet/range "sheet_range",
    using the credential file in the path "gsheet_cred_path" (generated by google spreadsheet API)
    and returns a dict considering the first row as keys (columns' titles)
    '''
    gsheet = GSheetAPI(gsheet_cred_path)
    values = gsheet.get_data_from_sheet(sheet_id=sheet_id, sheet_range=sheet_range)
    dados = []

    for l in range(1, len(values)): # número de linhas de dados
        dict = {}
        for c in range(0, len(values[0])): # número de colunas
            dict[values[0][c]] = values[l][c]
        dados.append(dict)
    return dados

def get_params_produtos(origem:str) -> List[Dict[str,str]]:
    products_params = []
    if origem == 'gsheet':
        logging.info(f'Carregando dados da planilha google...')
        products_params = get_dict_from_gsheet(
            sheet_id = get_sheet_id_from_json(json_cred_path),
            gsheet_cred_path = gsheet_cred_path,
            sheet_range = 'busca!A:K',            
        )
    elif origem == 'local':
        logging.info(f'Carregando dados da planilha local: {busca_xlsx_path}...')
        products_params = get_dict_from_xls(busca_xlsx_path)
    
    return products_params

def busca_OLX(products_params : List[Dict[str,str]], paralelizar = True):
    logging.info(f'Iniciando busca...')
    if paralelizar:
        run_func_in_parallel_Process(busca_produto_e_envia_email, products_params)
        # run_func_in_parallel_ThreadPool(busca_produto_e_envia_email, products_params)
    else:
        for params in products_params:
            busca_produto_e_envia_email(params)

    logging.info("Finalizou todas as pesquisas!")


if __name__ == '__main__':

    if len(sys.argv) == 1:
        origem = ORIGEM_DEFAULT
    elif sys.argv[1] == "--gsheet":
        origem = 'gsheet'
    elif sys.argv[1] == "--local":
        origem = 'local'
    else:
        logging.error(f"Argumentos inválidos: {sys.argv[1:]}")
        sys.exit()

    source_dir = os.path.dirname(__file__)
    templates_dir = os.path.join(source_dir,'templates')
    busca_xlsx_path_template = os.path.join(templates_dir,BUSCA_FILE)
    json_cred_path_template = os.path.join(templates_dir,CRED_FILE)

    json_cred_path = os.path.join(source_dir, CRED_FILE)
    busca_xlsx_path = os.path.join(source_dir, BUSCA_FILE)
    gsheet_cred_path = os.path.join(source_dir, GSHEET_CRED_FILE)
    flag_exit = False

    if not os.path.exists(json_cred_path):
        logging.info(f'Edite o arquivo "{CRED_FILE}" e depois execute novamente')
        shutil.copy2(json_cred_path_template, json_cred_path)
        flag_exit = True

    if origem == 'gsheet':
        if not os.path.exists(gsheet_cred_path):
            logging.info(f'Crie o arquivo "{GSHEET_CRED_FILE}" e depois execute novamente')
            flag_exit = True
    else:
        if not os.path.exists(busca_xlsx_path):
            logging.info(f'Edite o arquivo "{BUSCA_FILE}" e depois execute novamente')
            shutil.copy2(busca_xlsx_path_template, busca_xlsx_path)
            flag_exit = True

    if flag_exit:
        sys.exit()

    try:
        while True:
            with Timer() as t:
                hora_atual = time.localtime().tm_hour
                products_params = get_params_produtos(origem = origem)
                logging.debug(products_params)
                busca_OLX(products_params = products_params, paralelizar = PARALELIZAR)
                logging.info(f'Tempo total das buscas: {t.elapsed}s')
            if LOOP_SECONDS:
                time.sleep(LOOP_SECONDS)
            else:
                break      
    except:
        logging.error(traceback.format_exc())
