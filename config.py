LOOP_SECONDS = 8*60*60 # Intervalo para re-execução da busca (0 desativa loop)
PARALELIZAR = True # paralelizar busca (desative para debugar log)
ORIGEM_DEFAULT = 'gsheet' # origem dos dados quando não é passada como argumento na chamada
CRED_FILE = 'cred.json' # arquivo com credenciais de e-mail e o id do da planilha do google
BUSCA_FILE = 'busca.xlsx' # planilha local com dados de produtos a serem buscados
GSHEET_CRED_FILE = 'gsheet_credentials.json' # arquivo com credenciais do google sheets (gerado em https://console.cloud.google.com/apis/credentials)
