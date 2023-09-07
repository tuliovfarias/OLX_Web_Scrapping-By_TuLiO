# Use a imagem oficial do Python como base
FROM python:3.9

# Defina o locale no sistema
RUN apt-get update && apt-get install -y locales locales-all

# Defina o locale padrão
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR:en
ENV LC_ALL pt_BR.UTF-8

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie os arquivos de código fonte e o arquivo requirements.txt
COPY . /app

# Instale as dependências da aplicação
RUN pip install --no-cache-dir -r requirements.txt

# Roda a aplicação
CMD ["python", "html_scraping.py"]