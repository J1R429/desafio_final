#rodar no jupyter e codespace
# docker ps
#docker cp <nome_do_container>:/home/jovyan ./jupyter


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

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
output_path = "s3://bucketdesafiofinal/Refined"




# junção dataframes

df_movie = df_movie.withColumnRenamed("imdb_id", "movie_imdb_id")
df_geral = df_tmdb.join(df_movie, df_movie.movie_imdb_id == df_tmdb.imdb_id, 'inner')


# Removendo linhas duplicadas com base na coluna imdb_id para garantir que seja uma chave primária
df_geral = df_geral.dropDuplicates(["imdb_id"])
# Criando uma view temporária a partir do DataFrame geral
df_geral.createOrReplaceTempView('df_geral_view')

# Visualizar o esquema do DataFrame df_geral
df_geral.printSchema()



# Definir as consultas para as tabelas

df_data_query = """
    SELECT anoLancamento_tmdb AS Ano_Lancamento 
    FROM df_geral_view
"""



df_titulo_query = """
    SELECT tituloPrincipal AS Titulo, 
        imdb_id as titulo_imdb_id
    FROM df_geral_view
"""

df_adulto_query = """
    SELECT adulto AS Adulto, 
        imdb_id as adulto_imdb_id
    FROM df_geral_view
"""

# Criar DataFrames usando as consultas
df_data = spark.sql(df_data_query)
df_titulo = spark.sql(df_titulo_query)
df_adulto = spark.sql(df_adulto_query)


df_data.printSchema()
df_titulo.printSchema()
df_adulto.printSchema()


# Adicionar uma nova coluna de ID em cada DataFrame
df_data = df_data.withColumn("id_data", monotonically_increasing_id()+1)
df_titulo = df_titulo.withColumn("id_titulo", monotonically_increasing_id()+1)
df_adulto = df_adulto.withColumn("id_adulto", monotonically_increasing_id()+1)


df_fato_query = """
    SELECT DISTINCT
        df_geral_view.imdb_id as fato_imdb_id, 
        orcamento, 
        receita, 
        tempoMinutos, 
        id_genero, 
        popularidade, 
        contagem_de_voto, 
        votacao_media, 
        data_coleta_tmdb,
        anoLancamento_tmdb,
        adulto
    FROM df_geral_view
"""
df_fato = spark.sql(df_fato_query)


df_fato.printSchema()

# Converter id_adulto para string
df_adulto = df_adulto.withColumn("id_adulto", col("id_adulto").cast("string"))
df_data = df_data.withColumn("id_data", col("id_data").cast("string"))
df_titulo = df_titulo.withColumn("id_titulo", col("id_titulo").cast("string"))



df_fato = df_fato.join(df_data.select('id_data', 'Ano_Lancamento'),
                       df_fato.anoLancamento_tmdb==df_data.Ano_Lancamento, 'inner')
#df_fato = df_fato.drop('anoLancamento_tmdb', 'Ano_Lancamento')
                                    

df_fato = df_fato.join(df_titulo.select('id_titulo', 'titulo_imdb_id'),
                       df_fato.fato_imdb_id==df_titulo.titulo_imdb_id, 'inner')
#df_fato = df_fato.drop('titulo_imdb_id')


df_fato = df_fato.join(df_adulto.select('id_adulto', 'adulto_imdb_id'),
                       df_fato.fato_imdb_id==df_adulto.adulto_imdb_id, 'inner')
#df_fato = df_fato.drop('adulto', 'Adulto')


df_temp = df_fato.toPandas()
drop_colunas = ['anoLancamento_tmdb', 'Ano_Lancamento', 'titulo_imdb_id', 'adulto', 'adulto_imdb_id']
df_temp = df_temp.drop(drop_colunas, axis=1)
df_fato = spark.createDataFrame(df_temp)



df_fato.orderBy('fato_imdb_id').show
df_fato.printSchema()



# Escrever DataFrames como tabelas no AWS Glue
def salvar_tabela(df, tabela_nome):
    df.write.format("parquet") \
        .mode("overwrite") \
        .save(output_path + "/" + tabela_nome)


# Salvando as tabelas
salvar_tabela(df_fato, "tabela_fato")
salvar_tabela(df_data, "tabela_data")
salvar_tabela(df_titulo, "tabela_titulo")
salvar_tabela(df_adulto, "tabela_adulto")



# Encerrar a sessão do Spark
spark.stop()
job.commit()