# OLX-Web-Scrapping by Túlio Farias

Busca automatizada de produtos no site da OLX usando planilha para cadastro de produtos/filtros

## Para usar planilha local assista:
- https://www.youtube.com/watch?v=bqVZsbjeJ2Y
- Execute o script `html_scraping.py` com o argumento --local, exemplo: `python3 html_scraping.py --local`

## Para usar planilha do Google Sheets, siga os passos (vídeo, em breve):
1. No Google Cloud console, vai em Menu menu > APIs & Services > Credentials.
ou entre direto no link: https://console.cloud.google.com/apis/credentials

2. Clique em `Create Credentials` > `OAuth client ID`.

3. Clique em `Application type` > `Desktop app`.

4. No campo de nome, escreva um nome para a credencial. Este nome será mostrado somente no Google Cloud console.

5. Clique em `Create`. A tela de OAuth client criado aparecerá com seu `Client ID` e `Client secret`.

6. Clique em OK. A nova credencial irá aparecer em OAuth 2.0 Client IDs.

7. Baixe o arquivo JSON e salve com nome `gsheet_credentials.json` na mesma pasta do projeto

8. Crie uma planilha no Google sheets com as mesmas colunas da `/templates/busca.xlsx` com nome da página: `busca` e preencha com os produtos e filtros desejados

9. Execute o script `html_scraping.py`. Na primeira execução, será criado o arquivo `/cred.json`

10. Edite o arquivo `/cred.json` com as informações do e-mail e o id da planilha do Google Sheets.

11. Pronto, agora, execute o script `html_scraping.py` com o argumento --gsheet ou sem argumento, exemplo: `python3 html_scraping.py`
    - Será mostrado um log com os resultados da busca e será enviado, para cada produto, um e-mail para o destinatário cadastrado em `/cred.json`
    - Pode ser agendada uma tarefa no task scheduler (windows) ou crontab (linux) para automatizar as buscas