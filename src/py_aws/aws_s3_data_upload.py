import boto3
from   boto3.s3.transfer import TransferConfig
import logging 
from   botocore.exceptions import ClientError
import os 


WORK_DIR = os.getcwd()
DATA_DIR = os.path.join(WORK_DIR, 'data')
S3_BUCKET = 'py-datalake-dev'

s3_client = boto3.client('s3')

# Lista todos os Buckest ao usuário
def list_all_buckets():

    for buckets in s3_client.buckets.all():
        print('Lista de Buckets')
        print('* ', buckets.name)


##
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_data():


    """
        Otimizado para uploado de grandes arquivos.
        in : recebe como parâmetro caminho da pasta com os arquivos
        out: lista arquivos no bucket após o upload
    """

    # CONFIGURAÇÕES DA TRANSFERÊNCIA DE DADOS
    config = TransferConfig(    

                                multipart_threshold=1024*25, 
                                max_concurrency=10,
                                multipart_chunksize=1024*25, 
                                use_threads=True
                            )
    
    # CARGA DE DADOS
    files_dir = os.path.join(DATA_DIR,'olist')                      # Lista Arquivos no Diretório de Dados

    for file_name in os.listdir(files_dir):                         # Itera e realiza upload de cada arquivo                                 
        
        file_path = os.path.join(files_dir,file_name)
        key_path  = 'olist/' +  file_name  

        # Carrega Arquivos no S3
        s3_client.upload_file(
                                file_path, 
                                S3_BUCKET, 
                                key_path, 
                                Config    = config
                                
        )
        print('* Arquivo {} carregado '.format(file_name))
    
    print()
    print('Carga de dados realizado com Sucesso')


   
#upload_file(file_name=os.path.join(DATA_DIR, 'adult.csv'),bucket='py-datalake-dev', object_name='adult.csv')
#list_all_buckets()
upload_data()


