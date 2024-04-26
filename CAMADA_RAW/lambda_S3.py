import requests
import sys 
import os
import csv
from datetime import datetime
import pandas as pd
import json
import boto3
import concurrent.futures

def lambda_handler(event, context):

    s3= boto3.client('s3')
     
    # Nome do bucket e caminho do arquivo no S3
    bucket_name = "bucketdesafiofinal"
        
    
    ###########################################################################################################

    #CRIANDO DATA FRAME DE POPULARIDADE GERAL

    # IDs dos gêneros desejados
    ids_genero = [14, 878]
    # Número máximo de páginas por ano
    max_paginas_por_ano = 500
    # Dicionário para armazenar os DataFrames de filmes por ano e por gênero
    df_por_ano_genero = {}

    # Loop pelos anos
    for ano in range(2001, 2016):
        resultados_por_ano_genero = {'Ano': [], 'Genre_ID': [], 'id': [], 'title': [], 'release_date': [], 'popularity': [], 'vote_average': [], 'vote_count': [], 'adult':[]}
        # Loop pelos IDs de gênero
        for id_genero in ids_genero:
            # Loop pelas páginas
            for pagina in range(1, max_paginas_por_ano + 1):
                # URL base da API do TMDb para pesquisa de filmes por gênero
                base_url = "https://api.themoviedb.org/3/discover/movie"
                # Parâmetros da pesquisa
                params = {
                    "api_key": "XXXXXXX",
                    "with_genres": id_genero,
                    "primary_release_year": ano,
                    "page": pagina
                }
                
                # Fazer a solicitação à API
                response = requests.get(base_url, params=params)
                
                # Verificar se a solicitação foi bem-sucedida
                if response.status_code == 200:
                    # Converter a resposta para JSON
                    data = response.json()
                    # Adicionar os resultados à lista
                    for result in data.get('results', []):
                        resultados_por_ano_genero['Ano'].append(ano)
                        resultados_por_ano_genero['Genre_ID'].append(id_genero)
                        resultados_por_ano_genero['id'].append(result['id'])
                        resultados_por_ano_genero['title'].append(result['title'])
                        resultados_por_ano_genero['release_date'].append(result['release_date'])
                        resultados_por_ano_genero['popularity'].append(result['popularity'])
                        resultados_por_ano_genero['vote_average'].append(result['vote_average'])
                        resultados_por_ano_genero['vote_count'].append(result['vote_count'])
                        resultados_por_ano_genero['adult'].append(result['adult'])
                    
                    # Verificar se atingiu a última página
                    if pagina >= data['total_pages']:
                        break
                else:
                    print("Erro ao fazer a solicitação:", response.status_code)
                    break
        
        # Criar um DataFrame com os resultados do ano atual
        df_por_ano_genero[ano] = pd.DataFrame(resultados_por_ano_genero)

    # Juntar todos os DataFrames em um único DataFrame
    df_tmdb = pd.concat(df_por_ano_genero.values(), ignore_index=True)

    # Remover itens duplicados com base na coluna 'id'
    df_tmdb.drop_duplicates(subset='id', inplace=True)

    # Exibir o DataFrame final
    print(df_tmdb)

    ######################################################################################################

    # Função para consultar o orçamento de um filme por ID
    def consultar_dados_por_id(movie_id):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
        params = {
            "api_key": "XXXXXXX"
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return {
                'budget': data.get('budget', None),
                'imdb_id': data.get('imdb_id', None),
                'runtime': data.get('runtime', None),
                'revenue':data.get('revenue', None),
            }
        else:
            print(f"Erro ao consultar o filme com ID {movie_id}. Status code: {response.status_code}")
            return None


    # Função para processar consultas em paralelo
    def processar_consultas(movie_ids):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return list(executor.map(consultar_dados_por_id, movie_ids))

    # Lista de IDs de filmes
    movie_ids = df_tmdb['id'].tolist()


    # Consulta dos dados em paralelo
    dados_filmes = processar_consultas(movie_ids)


    # Criando DataFrame com os dados consultados
    df_dados_filmes = pd.DataFrame(dados_filmes)


    # Redefinindo o índice do DataFrame df_tmdb
    df_tmdb.reset_index(drop=True, inplace=True)

    # Redefinindo o índice do DataFrame df_dados_filmes
    df_dados_filmes.reset_index(drop=True, inplace=True)


    # Criando DataFrame com os dados consultados
    df_dados_filmes = pd.DataFrame(dados_filmes)

    # Combinando os DataFrames
    df_tmdb = pd.concat([df_tmdb, df_dados_filmes], axis=1)


    # Exibindo o DataFrame com a nova coluna
    print(df_tmdb)

   
           
    
    ########################################################################################################################
    # JSON GERAL

    # Definir o limite de registros desejado
    limite_registros = 100  # Limite de registros por arquivo

    # Verificar o número total de registros no DataFrame
    num_registros = len(df_tmdb)

    # Lista para armazenar os DataFrames menores
    dataframes_divididos = []

    # Verificar se o número total de registros excede o limite
    if num_registros > limite_registros:
        print("Número total de registros excede 100. Dividindo em arquivos menores.")

        # Dividir os dados em DataFrames menores com no máximo 100 registros
        num_dataframes = num_registros // limite_registros
        resto = num_registros % limite_registros

        inicio = 0
        for i in range(num_dataframes):
            fim = inicio + limite_registros
            df_temp = df_tmdb.iloc[inicio:fim]
            dataframes_divididos.append(df_temp)
            inicio = fim

        # Adicionar o DataFrame restante, se houver
        if resto > 0:
            df_temp = df_tmdb.iloc[inicio:]
            dataframes_divididos.append(df_temp)

        # Escrever todos os dados em um único arquivo JSON
        for i, item in enumerate(dataframes_divididos):
            corpo_obj = item.to_json(orient='records', lines=True)
            s3_chave = f'Raw/tmdb/json/2024/04/11/geral/parte_{i}.json'
            s3.put_object(Bucket=bucket_name, Key=s3_chave, Body=corpo_obj)