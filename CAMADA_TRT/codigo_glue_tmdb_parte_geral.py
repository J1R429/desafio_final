import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType

# Inicializar o SparkSession
spark = SparkSession.builder \
    .appName("Transformação TMDb") \
    .getOrCreate()



# Inicializar o contexto do Glue
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)



source_file = "s3://bucketdesafiofinal/Raw/TMDB/json/2024/04/12/"
df_tmdb = spark.read.json(source_file)


# Remover a coluna 'release_date'
df_tmdb = df_tmdb.drop("release_date")

# Renomear as colunas conforme especificado
df_tmdb = df_tmdb.withColumnRenamed("Ano", "anoLancamento_tmdb") \
    .withColumnRenamed("Genre_ID", "id_genero") \
    .withColumnRenamed("id", "id_tmdb") \
    .withColumnRenamed("title", "titulo") \
    .withColumnRenamed("popularity", "popularidade") \
    .withColumnRenamed("vote_average", "votacao_media") \
    .withColumnRenamed("vote_count", "contagem_de_voto") \
    .withColumnRenamed("adult", "adulto") \
    .withColumnRenamed("budget", "orcamento")\
    .withColumnRenamed("runtime", "duracao")\
    .withColumnRenamed("revenue", "receita")

# Converter a coluna para inteiro e remover linhas com valores 0
df_tmdb = df_tmdb.withColumn("orcamento", df_tmdb["orcamento"].cast(IntegerType()))
df_tmdb = df_tmdb.filter(df_tmdb["orcamento"] > 0)

# Converter a coluna 'orcamento' para inteiro e remover linhas com valores 0
df_tmdb = df_tmdb.withColumn("receita", df_tmdb["receita"].cast(IntegerType()))
df_tmdb = df_tmdb.filter(df_tmdb["receita"] > 0)

# Remover linhas com 'contagem_de_voto' menores que 30 e tempo menos que 80
df_tmdb = df_tmdb.withColumn("contagem_de_voto", df_tmdb["contagem_de_voto"].cast(IntegerType()))
df_tmdb = df_tmdb.filter(df_tmdb["contagem_de_voto"] >= 30)

df_tmdb = df_tmdb.withColumn("duracao", df_tmdb["duracao"].cast(IntegerType()))
df_tmdb = df_tmdb.filter(df_tmdb["duracao"] >= 80)


# Remover linhas com 'imdb_id' vazias
df_tmdb = df_tmdb.filter(df_tmdb["imdb_id"].isNotNull())



# Reiniciar o índice
df_tmdb = df_tmdb.withColumn("index", col("imdb_id"))  # Criar uma nova coluna 'index' com os valores de 'imdb_id'
df_tmdb = df_tmdb.drop("imdb_id")  # Remover a coluna 'iimdb_id'
df_tmdb = df_tmdb.withColumnRenamed("index", "imdb_id")  # Renomear a coluna 'index' para 'imdb_id'
df_tmdb = df_tmdb.orderBy("imdb_id")  # Ordenar o DataFrame por imdb_id'



data_extracao = '2024/04/12'
df_tmdb = df_tmdb.withColumn("data_coleta_tmdb", lit(data_extracao))


# Caminho de saída para os dados no formato Parquet
output_path = "s3://bucketdesafiofinal/TRT/TMDB/Fantasy/dt=2024-04-12"


# Particionar os dados e escrever em arquivos Parquet
df_tmdb.write.parquet(output_path, mode="overwrite")




# Encerrar a sessão do Spark
spark.stop()



job.commit()