
from ast import While
import pandas as pd
import os
from   time import time
from   datetime import datetime

# Database Integration
import sqlalchemy as db
from   sqlalchemy.exc import SQLAlchemyError

# Importando arquivo de configurações com variáveis de ambiente sensíveis
from dotenv import load_dotenv
load_dotenv()

WORK_DIR = os.getcwd()
DATA_DIR = os.path.join(WORK_DIR,'data')


# Gera conexão com o SGBD - MySQL
def mysql_connection():

    # in  - Não recebe nenhum parâmetro
    # out - return connection - retorna conexão validada com o banco de dados

    dialect  = 'mysql'
    driver   = 'pymysql'
    user     = os.getenv("USER_DB")
    password = os.getenv("PASSWORD")
    host     = 'localhost'
    port     = 3306
    database = 'ny_taxi'

    # String de conexão com o SGBD
    string = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}'
    
    connection = None

    try:
        engine = db.create_engine(string)
        connection = engine.connect()
        metadata = db.MetaData()
        print('Conexão realizada com sucesso ')

    except SQLAlchemyError as e:
        print("Error ao se conectar ao MySQL: ",e )
    
    return connection

    
# Padroniza o nome das tabelas
def padroniza_tabela(file_name):

    #in  - filename: recebe o nome dos arquivos a serem padronizados
    #out - retorna o nome da tabela padronizado    
    
    table_name = "tb_" + file_name.strip('.csv').replace('olist_','').replace('_dataset','')

    return table_name

# Carga de dados - upload das tabelas
def data_upload():

    connection = mysql_connection().execution_options( autocommit = True)

    for file_name in os.listdir(os.path.join(DATA_DIR,'olist')):

        if file_name.endswith('.csv'): # filtra somente arquivos csv
            
            # Padroniza nome da tabela
            table_name = padroniza_tabela(file_name)

            # Caminho até o arquivo
            file_path = os.path.join(DATA_DIR,'olist',file_name)  

            # Criando um iterador para ler data em Chunks   
            df_iter = pd.read_csv(file_path, iterator=True, chunksize= 10000)    

            # Se tabela já existir overwrite
            df = pd.read_csv(file_path, nrows=500)
            df['upload_datetime'] = datetime.now().isoformat()
            df.head(n=0).to_sql(con = connection, name= table_name, if_exists='replace')            

            cont = 0 # contador de chunks
            go = True

            try:
                # Carregando Dados em Chunks
                while go:
                    
                    cont+=1
                    #monitora tempo de carga
                    t_start = time()

                    df_tmp = next(df_iter)
                    df_tmp['upload_datetime'] = datetime.now().isoformat()

                    df_tmp.to_sql(name = table_name, con = connection, if_exists='append')

                    t_end = time()
                    print('  - >  Carga número {:02d} realizada com sucesso em {:.3f} segundos'.format(cont,t_end - t_start))            
                        
            except StopIteration:
                go = False

            print()
            print('* Tabela {} carregada ... '.format(table_name))
            
    print()
    print('Carga de Dados executada com Sucesso')

data_upload()

