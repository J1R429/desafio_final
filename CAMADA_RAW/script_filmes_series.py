import boto3
import os
import csv
from datetime import datetime

def load_data_to_aws(filename, bucket_name, storage_layer):
    session = boto3.Session(
        aws_access_key_id='ASIAJM5R',
        aws_secret_access_key='oazJV4ZQleg3fk5',
        aws_session_token= 'IQo8dqmp9psQ=='
    )
    # Inicializar o cliente S3
    s3_client = session.client('s3')
    
    # Obter a data de processamento atual
    current_date = datetime.now().strftime("%Y/%m/%d")
    
    # Nome do arquivo
    base_name = os.path.basename(filename)
    
    # Montar o caminho de destino no S3
    destination_path = f"{storage_layer}/{current_date}/{base_name}"
    
    try:
        # Carregar o arquivo para o S3
        s3_client.upload_file(filename, bucket_name, destination_path)
        print(f"Arquivo {filename} carregado com sucesso para o S3 em {bucket_name}/{destination_path}")
    except Exception as e:
        print(f"Erro ao carregar o arquivo para o S3: {e}")


# Definir o nome dos arquivos CSV
movies_filename = 'data/movies.csv'
series_filename = 'data/series.csv'

# Definir o nome do bucket S3
bucket_name = 'bucketdesafiofinal'

# Definir as camadas de armazenamento
storage_layer_movies = 'Raw/Local/CSV/Movies'
storage_layer_series = 'Raw/Local/CSV/Series'

# Definir a origem dos dados
data_origin_movies = 'Movies'
data_origin_series = 'Series'

# Definir o formato dos dados
data_format = 'CSV'

# Carregar filmes para o S3
load_data_to_aws(movies_filename, bucket_name, storage_layer_movies)

# Carregar séries para o S3
load_data_to_aws(series_filename, bucket_name, storage_layer_series)
