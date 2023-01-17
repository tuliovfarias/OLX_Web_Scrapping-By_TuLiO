import subprocess
import sys

def install_requirements(requirements_file):
    subprocess.check_output([sys.executable, "-m", "pip", "install", "-r", requirements_file])
requirements_file = "requirements.txt"
install_requirements(requirements_file)

from multiprocessing import Manager, Process
import shutil
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import re
from urllib.parse import quote
import traceback
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from retry import retry
import logging
from typing import List, Dict, Tuple
import json
import concurrent.futures      


logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


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

        dict_cidade = {'bh': 'belo-horizonte-e-regiao', 'sp': 'sao-paulo-e-regiao'}
        dict_ordenar_por = {'data' : '&sf=1'}
        dict_estado = {'bh': 'mg', 'sp': 'sp'}

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

        logging.info(f"\n******* PESQUISANDO: {self.produto} ({'%s' % ','.join(self.texto_pesquisa)})")
        manager = Manager()
        self.lista_produtos = manager.list()
        run_func_in_parallel_Process(self._OLX_pesquisa, self.texto_pesquisa)
        # for pesquisa in self.texto_pesquisa:
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
        # logging.info(f'\t- {pesquisa}:')        
        for pagina in range(1, self.max_paginas+1):
            url = self.criar_url_base(pesquisa)
            page_found_flag = self.OLX_pesquisa_pagina(url, pagina, pesquisa)
            if not page_found_flag: break
        
    def criar_url_base(self, pesquisa:str) -> str:
        url = 'https://'+ self.estado + 'olx.com.br/' + self.cidade + '?q=' + quote(pesquisa) + self.ordenar_por
        return url
           
    def OLX_pesquisa_pagina(self, url:str, pagina:int, pesquisa:str):
        url = url+'&o='+str(pagina)

        logging.info(f'\tPÁGINA {str(pagina)} ({pesquisa}) - {url}')

        page = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(page.content, "html.parser")
        produtos = soup.find('ul', {"id": ["ad-list"]})

        if produtos == None or len(produtos)== 0:
            logging.debug(f'\t\t\tNenhum resultado na página {str(pagina)}')
            return False

        produtos = produtos.findAll('li',recursive=False)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for produto in produtos:
                produto:BeautifulSoup

                try:
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
                except TypeError:
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
            page_produto = requests.get(url_produto, headers=headers)
            soup_produto = BeautifulSoup(page_produto.content, "html.parser") # features="lxml"

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
                    logging.error(f"Não encontrou a data de publicação do produto: {url_produto}/n{dados_str}/n/n")
                    return
            logging.debug(f'Data/hora de publicação: {data_hora_post}')

            group_or_empty = lambda a: a.group() if a else ''

            titulo_anuncio = re.search(r'(?<=\"subject\":\").*?(?=\")',dados_str)
            titulo_anuncio = group_or_empty(titulo_anuncio)
            logging.debug(f'Título = {titulo_anuncio}')

            preco_post = re.search(r'(?<=\"price\":\"R\$ ).*?(?=\")',dados_str)
            preco_post = group_or_empty(preco_post).replace('.','')
            preco_post = float(preco_post) if preco_post else 0.0
            logging.debug(f'Preço = {preco_post}')

            cidade_post = re.search(r'(?<=\"Município\",\"value\":\").*?(?=\")',dados_str)
            cidade_post = group_or_empty(cidade_post)
            logging.debug(f'Cidade = {cidade_post}')

            bairro_post = re.search(r'(?<=\"Bairro\",\"value\":\").*?(?=\")',dados_str)
            bairro_post = group_or_empty(bairro_post)
            logging.debug(f'Bairro = {bairro_post}')

            estado_post = url_produto[8:10].upper()                  
            logging.debug(f'Estado = {estado_post}')

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
            logging.ERROR(traceback.format_exc())
            # count_errors = count_errors+1

    def filtrar(self, preco_max=None, intervalo=None):
        self.preco_max = preco_max
        today = datetime.now()
        if self.lista_produtos:
            if preco_max:
                logging.info(f'Filtrando preços menores que: {preco_max}')
                # self.df_lista_produtos = [self.df_lista_produtos['preco'] <= preco_max]
                self.df_lista_produtos.query(f'preco <= {preco_max}', inplace=True)
            if intervalo:
                self.intervalo_timedelta, self.intervalo_tipo_str = self.format_intervalo(intervalo)
                logging.info(f"Filtrando datas maiores que: {(today-self.intervalo_timedelta).strftime('%d/%m/%Y %H:%M:%S')}")
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

    @retry(tries=5, delay=60)
    def EnviarEmail(self, json_cred_path, email_para : str = None):
        with open(json_cred_path) as cred:
            dados = json.load(cred)
            host = dados["e-mail"]["host"]
            port = dados["e-mail"]["port"]
            user = dados["e-mail"]["user"]
            password = dados["e-mail"]["password"]
            email_de = dados["e-mail"]["from"]
            if not email_para: 
                email_para = email_de
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
                    <p>Resultados:</p>
                """
        if self.preco_max:
            html = html + f"<p>- preço abaixo de: R$ {self.preco_max},00</p>"
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

def busca_produto_e_envia_email(dado:dict) -> Tuple[str, pd.DataFrame]:
    busca = BuscaProduto(**dado)
    lista_produtos = busca.OLX()
    # logging.info(lista_produtos.to_dict(orient = "records"))
    df_lista_filtrada = busca.filtrar(preco_max = float(dado['preco_max']), intervalo = dado['intervalo'])
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

def busca_OLX(paralelizar = False):
    product_params_dict = get_dict_from_xls(xlsx_filters_path)
    logging.info(f'Iniciando busca...')

    if paralelizar:
        run_func_in_parallel_Process(busca_produto_e_envia_email, product_params_dict) # pode dar erro dependendo da quantidade se buscas
        # results = run_func_in_parallel_ThreadPool(busca_produto_e_envia_email, product_params_dict) #error
    else:
        for params in product_params_dict:
            busca_produto_e_envia_email(params)

    logging.info("Finalizou todas as pesquisas!")

if __name__ == '__main__':
    
    source_dir = os.path.dirname(__file__)
    xlsx_filters_path = os.path.join(source_dir,'busca.xlsx')
    xlsx_filters_path_example = os.path.join(source_dir,'busca_exemplo.xlsx')
    json_email_cred_path = os.path.join(source_dir,'cred.json')
    json_email_cred_path_example = os.path.join(source_dir,'cred_exemplo.json')
    flag_exit = False


    if not os.path.exists(json_email_cred_path):
        logging.info(f'Edite o arquivo "cred.json" e depois execute novamente')
        shutil.copy2(json_email_cred_path_example, json_email_cred_path)
        flag_exit = True
    if not os.path.exists(xlsx_filters_path):
        logging.info(f'Edite o arquivo "busca.xlsx" e depois execute novamente')
        shutil.copy2(xlsx_filters_path_example, xlsx_filters_path)
        flag_exit = True
    if flag_exit:
        exit()

    from contexttimer import Timer
    try:
        with Timer() as t:
            busca_OLX(paralelizar = False)
            logging.info(f'Tempo total das buscas: {t.elapsed}s')
    except:
        logging.error(traceback.format_exc())
