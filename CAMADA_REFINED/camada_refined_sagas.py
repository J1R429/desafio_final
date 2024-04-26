import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, concat, lit
from pyspark.sql.window import Window

# Inicializar o SparkSession
spark = SparkSession.builder \
    .appName("Transformação TRT") \
    .getOrCreate()

# Inicializar o contexto do Glue
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

source_file_movie = "s3://bucketdesafiofinal/TRT/Movies/"
source_file_tmdb = "s3://bucketdesafiofinal/TRT/TMDB/Fantasy/dt=2024-04-12/"

# Carregar dados do arquivo Parquet
df_tmdb = spark.read.parquet(source_file_tmdb)
df_movie = spark.read.parquet(source_file_movie)

# Caminho de saída para os dados no formato Parquet
output_path = "s3://bucketdesafiofinal/Refined2"


df_movie = df_movie.withColumnRenamed("imdb_id", "movie_imdb_id")



# junção dataframes
df_geral = df_movie.join(df_tmdb, df_movie.movie_imdb_id == df_tmdb.imdb_id, 'inner')


# Remover linhas duplicadas com base na coluna movie_imdb_id para garantir que seja uma chave primária
df_geral = df_geral.dropDuplicates(["movie_imdb_id"])

# Criando uma view temporária a partir do DataFrame geral
df_geral.createOrReplaceTempView('df_geral_view')

# Visualizar o esquema do DataFrame df_geral
df_geral.printSchema()

# Definir as consultas para as tabelas
ids_sagas = ['tt0241527', 'tt0295297', 'tt0304141', 'tt0330373', 'tt0373889', 'tt0417741', 'tt0926084', 'tt1201607', 'tt1392170', 'tt1951264', 'tt1951265', 'tt1951266', 'tt1099212', 'tt1259571', 'tt1324999', 'tt1325004', 'tt1673434']



ids_sagas_str = ", ".join([f"'{id}'" for id in ids_sagas])
df_sagas_data_query = f"""
    SELECT anoLancamento_tmdb AS Ano_Lancamento 
    FROM df_geral_view
    WHERE movie_imdb_id IN ({ids_sagas_str})
"""


# Montar a parte da consulta SQL para buscar os títulos das sagas específicas
ids_sagas_str = ", ".join([f"'{id}'" for id in ids_sagas])
df_sagas_titulo_query = f"""
    SELECT tituloPrincipal AS Titulo, 
        movie_imdb_id as ts_imdb_id
    FROM df_geral_view
    WHERE movie_imdb_id IN ({ids_sagas_str})
"""


# Montar a parte da consulta SQL para buscar os títulos das sagas específicas
ids_sagas_str = ", ".join([f"'{id}'" for id in ids_sagas])
df_sagas_adulto_query = f"""
    SELECT adulto AS Adulto, 
        movie_imdb_id as as_imdb_id
    FROM df_geral_view
    WHERE movie_imdb_id IN ({ids_sagas_str})
"""


# Criar DataFrames usando as consultas
df_sagas_data = spark.sql(df_sagas_data_query)
df_sagas_titulo = spark.sql(df_sagas_titulo_query)
df_sagas_adulto = spark.sql(df_sagas_adulto_query)



# Criar uma janela para particionar os dados por nada, resultando em uma partição única
# Definir a ordem na janela
window_data = Window.orderBy("Ano_Lancamento")
window_titulo = Window.orderBy("ts_imdb_id")
window_adulto = Window.orderBy("as_imdb_id")



# Adicionar uma nova coluna de ID em cada DataFrame
# Adicionar uma nova coluna de ID em cada DataFrame
df_sagas_data = df_sagas_data.withColumn("id_tabela_data", concat(lit("dt"), row_number().over(window_data)))
df_sagas_titulo = df_sagas_titulo.withColumn("id_tabela_titulo", concat(lit("ti"), row_number().over(window_titulo)))
df_sagas_adulto = df_sagas_adulto.withColumn("id_tabela_adulto", concat(lit("ad"), row_number().over(window_adulto)))


# Converter id_adulto para string
df_sagas_data = df_sagas_data.withColumn("id_tabela_data", col("id_tabela_data").cast("string"))
df_sagas_titulo = df_sagas_titulo.withColumn("id_tabela_titulo", col("id_tabela_titulo").cast("string"))
df_sagas_adulto = df_sagas_adulto.withColumn("id_tabela_adulto", col("id_tabela_adulto").cast("string"))

# Montar a parte da consulta SQL para buscar os dados das sagas específicas
ids_sagas_str = ", ".join([f"'{id}'" for id in ids_sagas])
df_fato_sagas_query = f"""
    SELECT DISTINCT
        df_geral_view.movie_imdb_id as fs_movie_imdb_id, 
        orcamento, 
        receita, 
        tempoMinutos, 
        popularidade, 
        contagem_de_voto, 
        votacao_media, 
        data_coleta_tmdb,
        anoLancamento_tmdb
    FROM df_geral_view
    WHERE df_geral_view.movie_imdb_id IN ({ids_sagas_str})
"""


df_fato_sagas = spark.sql(df_fato_sagas_query)


df_fato_sagas.printSchema()



# Joining and dropping redundant columns
df_fato_sagas = df_fato_sagas.join(df_sagas_data.select("id_tabela_data", "Ano_Lancamento"),
                                   df_fato_sagas.anoLancamento_tmdb == df_sagas_data.Ano_Lancamento, "inner")
#df_fato_sagas = df_fato_sagas.drop("anoLancamento_tmdb", "Ano_Lancamento")


df_fato_sagas = df_fato_sagas.join(df_sagas_titulo.select('id_tabela_titulo', 'ts_imdb_id'),
                       df_fato_sagas.fs_movie_imdb_id==df_sagas_titulo.ts_imdb_id, 'inner')
#df_fato_sagas = df_fato_sagas.drop('ts_imdb_id')


df_fato_sagas = df_fato_sagas.join(df_sagas_adulto.select('id_tabela_adulto', 'as_imdb_id'),
                       df_fato_sagas.fs_movie_imdb_id==df_sagas_adulto.as_imdb_id, 'inner')
#df_fato = df_fato.drop('adulto', 'Adulto')

# Remover colunas redundantes após o join
df_fato_sagas = df_fato_sagas.drop("anoLancamento_tmdb", "Ano_Lancamento", "adulto", "ts_imdb_id", "as_imdb_id")

# Eliminar linhas duplicadas
df_fato_sagas = df_fato_sagas.dropDuplicates(["fs_movie_imdb_id"])


df_fato_sagas.orderBy('fs_movie_imdb_id').show
df_fato_sagas.printSchema()


# Escrever DataFrames como tabelas no AWS Glue
def salvar_tabela(df, tabela_nome):
    df.write.format("parquet") \
        .mode("overwrite") \
        .save(output_path + "/" + tabela_nome)



# Salvando as tabelas
salvar_tabela(df_fato_sagas, "tabela_fato_sagas")
salvar_tabela(df_sagas_data, "tabela_sagas_data")
salvar_tabela(df_sagas_titulo, "tabela_sagas_titulo")
salvar_tabela(df_sagas_adulto, "tabela_sagas_adulto")



# Encerrar a sessão do Spark
spark.stop()
job.commit()