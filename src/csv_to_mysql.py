
import pandas as pd
import os
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

    """

    in  - Não recebe nenhum parâmetro
    out - return connection - retorna conexão validada com o banco de dados

    """

    dialect  = 'mysql'
    driver   = 'pymysql'
    user     = os.getenv("USER_DB")
    password = os.getenv("PASSWORD")
    host     = 'localhost'
    port     = 3306
    database = 'ny_taxi'

    # String de conexão com o SGBD
    string = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}'
    print(string)
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

    """
    in  - filename: recebe o nome dos arquivos a serem padronizados
    out - retorna o nome da tabela padronizado
    
    """

    table_name = "tb_" + file_name.strip('.csv').replace('olist_','').replace('_dataset','')

    return table_name

# Carga de dados - upload das tabelas
def data_upload():

    connection = mysql_connection().execution_options( autocommit = True)

    for file_name in os.listdir(os.path.join(DATA_DIR,'olist')):

        if file_name.endswith('.csv'):
            
            # Padroniza nome da tabela
            table_name = padroniza_tabela(file_name)

            # Gera o Dataframe        
            df_tmp = pd.read_csv(os.path.join(DATA_DIR,'olist',file_name))
            df_tmp['upload_datetime'] = datetime.now().isoformat()
        
            # Cria e carrega tabela
            df_tmp.to_sql(name=table_name, con= connection ,if_exists='replace')
            print('* Tabela {} carregada ... '.format(table_name))

    connection.close()
    print('Carga de Dados executada com Sucesso')

data_upload()

