# Crypto Data ETL pipeline with Airflow and AWS 

:us:

This project aims to **collect and update data on Crypto Tokens like Bitcoin and Ethereum** (in the future also Solana) in the form of CSV files covering large periods of trading data collected from the [Binance US API](https://docs.binance.us/#introduction).

Data collected includes: 

* **Token price**

* **Tokens bought and sold**

* **Number of aggregate transactions** (transactions of the same token and same price are aggregated together)

* **Net flow for the asset in a certain period**

All that in slices of 5 minutes of data each.


## Accessing the data 

* Dataset statistics (preview of the CSV and info like how many rows are there in the file and how many hours are covered) can be accessed  [here](https://jr6cd1g42j.execute-api.us-east-2.amazonaws.com/stage1/dashboard)

* Bitcoin dataset can be downloaded  [here](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=btc)

* Ethereum dataset can be downloaded  [here](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=eth)

All data and metadata is stored on **AWS S3** and links lead to a temporary URL where you can download certain files


## Architecture of the project

![](architecture.png)


There are 3 main scripts running on this application:

**Binance API functions (../crypto_data/dags/include/binance_api.py)**

These are functions that get **data from the binance API**, like Crypto Token price, amount of transactions and returns the data.

**CryptoDataETL class (../crypto_data/dags/include/crypto_data_etl.py)**

This class uses the above functions to **coordinate the extraction of Pandas Dataframes out of the API data**, with parameters such as Max hours of data that the dataset can have, how many minutes should be covered by each row, among others. This function provides 2 main public interfaces, **one function for creating a dataset** from scratch and one for **updating an existing dataset** with more and/or newer data.

**Airflow DAG for orchestrating the ETL pipeline (../crypto_data/dags/crypto_data.py)**

This DAG (Directed Acyclic Graph) implements the pipeline logic, including **branching logic** on the need to either create a dataset or update an existing one and handles cloud connections, such as **reading and uploading to an S3 bucket**.
Not only the CSV files are handled, but the JSON dataset metadata file, with info on the characteristics of the data, are also updated.

**OBS: Airflow project was created using the [AirflowCTL](https://github.com/kaxil/airflowctl) command line tool**

**Airflow DAG architecture**

![](airflow_dag.png)


## Setting up the project

### To run the project locally on Linux:

**1) create empty folder**

**2) copy file "local_setup.bash" from repo folder project_setup/ to created folder**

**3) change file variables $USER and $HOME_ to current terminal user and path to created folder**

**4) run bash files:**
```bash
bash local_setup.bash
```

**5) Follow instructions on file to add AWS connections to Airflow with AWS access ID and secret access keys**
This is to be done manually due to security reasons related to the AWS account and services

**6) To access Airflow UI just connect to Localhost 8080**

<br>


### To run the project on EC2 instances:

**1) create an EC2 instance, choose model with at least 2gb of RAM**

**1.1) Either during creating of after, open a custom TCP connection with all traffic allowed on port 8080 (for connecting to Airflow UI)**

**1.2) Edit the "user-data" config on the EC2 instance and copy and paste the "vm_setup.bash" file from the project_setup/ folder on the "user-data" space**

**2) Launch instance**

**3) Wait for a bit and see if installation and start of the airflow project was successful**

**4) Follow instructions on file to add AWS connections to Airflow with AWS access ID and secret access keys**
This is to be done manually due to security reasons related to the AWS account and services

**5) To access the Airflow UI, connect to the DNS URL of the instance on port 8080** 
(Connection url should look like `{instance-dns-url}:8080`)

<br>
<br>
<br>

# Pipeline de Dados de Cripto Moedas com Airflow e AWS

:brazil:

Este projeto visa **coletar e atualizar dados sobre Tokens de Criptomoedas como Bitcoin e Ethereum** (no futuro também Solana) na forma de arquivos CSV cobrindo grandes períodos de dados do mercado coletados da [API da Binance US](https://docs.binance.us/#introduction).

Os dados coletados incluem:

* **Preço do Token**

* **Tokens comprados e vendidos**

* **Número de transações agregadas** (transações do mesmo token e mesmo preço são agregadas juntas)

* **Fluxo líquido do ativo em um determinado período**

Tudo isso em periódos de 5 minutos de dados cada.

## Acessando os dados

* Estatísticas do dataset (prévia do CSV e informações como quantas linhas há no arquivo e quantas horas são cobertas no dataset) podem ser acessadas [aqui](https://jr6cd1g42j.execute-api.us-east-2.amazonaws.com/stage1/dashboard)

* O dataset do Bitcoin pode ser baixado [aqui](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=btc)

* O dataset do Ethereum pode ser baixado [aqui](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=eth)

Todos os dados e metadados são armazenados em um bucket do **AWS S3** e os links levam a uma URL temporária onde você pode baixar certos arquivos.

## Arquitetura do projeto

![](architecture.png)

Existem 3 scripts principais executando nesta aplicação:

**Funções da API da Binance (../crypto_data/dags/include/binance_api.py)**

Estas são funções que obtêm **dados da API da Binance**, como preço do Token de Criptomoedas, quantidade de transações e retorna os dados.

**Classe CryptoDataETL (../crypto_data/dags/include/crypto_data_etl.py)**

Esta classe usa as funções acima para **coordenar a extração de Dataframes do Pandas a partir dos dados da API**, com parâmetros como Máximo de horas de dados que o dataset pode ter, quantos minutos devem ser cobertos por cada linha, entre outros. Esta função fornece 2 principais interfaces públicas, **uma função para criar um dataset** do zero e outra para **atualizar um dataset existente**, aumentando o volume de dados e/ou atualizando os dados.

**DAG do Airflow para orquestrar a pipeline de ETL (../crypto_data/dags/crypto_data.py)**

Este DAG (Directed Acyclic Graph) implementa a lógica da pipeline, incluindo **lógica de ramificação** sobre a necessidade de criar um dataset ou atualizar um existente e lida com conexões na nuvem, como **leitura e upload para um bucket S3**.
Não apenas os arquivos CSV são atualizados, mas também o arquivo JSON de metadados do dataset é modificado para refletir o novo dataset.

**OBS: O projeto do Airflow foi criado usando a ferramenta de linha de comando [AirflowCTL](https://github.com/kaxil/airflowctl)**


**Arquitetura DAG do Airflow**

![](airflow_dag.png)

## Configurando o projeto

### Para executar o projeto localmente no Linux:

**1) crie uma pasta vazia**

**2) copie o arquivo "local_setup.bash" da pasta do projeto project_setup/ para a pasta criada**

**3) altere as variáveis do arquivo $USER e $HOME_ para o usuário atual do terminal e caminho para a pasta criada**

**4) execute os arquivos bash:**
```bash
bash local_setup.bash
```

**5) Siga as instruções no arquivo para adicionar conexões da AWS ao Airflow com ID de acesso da AWS e chaves de acesso secretas**
Isso deve ser feito manualmente devido a razões de segurança relacionadas à conta e serviços da AWS

**6) Para acessar a UI do Airflow, basta conectar ao Localhost 808.**

<br>

### Para executar o projeto em instâncias EC2:

**1) crie uma instância EC2, escolha um modelo com pelo menos 2GB de RAM**

**1.1) Durante a criação da instância ou depois, abra uma conexão TCP personalizada com todo o tráfego permitido na porta 8080 (para conectar à UI do Airflow)**

**1.2) Edite a configuração "user-data" na instância EC2 e copie e cole o arquivo "vm_setup.bash" da pasta project_setup/ no espaço "user-data"**

**2) Lance a instância**

**3) Aguarde um pouco e veja se a instalação e o início do projeto Airflow foram bem-sucedidos**

**4) Siga as instruções no arquivo para adicionar conexões da AWS ao Airflow com ID de acesso da AWS e chaves de acesso secretas**
Isso deve ser feito manualmente devido a razões de segurança relacionadas à conta e serviços da AWS

**5) Para acessar a UI do Airflow, conecte-se à URL DNS da instância na porta 8080**
(A URL de conexão deve parecer com `{url-dns-instancia}:8080`)


