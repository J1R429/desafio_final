import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Inicializar o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

source_file = "s3://bucketdesafiofinal/Raw/Local/CSV/Movies/2024/04/12/movies.csv"

# Caminho de saída para os dados no formato Parquet
output_path_final = "s3://bucketdesafiofinal/TRT/Movies/movies"


# Criar um DynamicFrame a partir do arquivo CSV
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="csv",
    format_options={"withHeader": True, "separator": "|"},
    transformation_ctx="nome_transformacao"
)

# Converter DynamicFrame para DataFrame
df = df.toDF()

# Preencher valores NaN na coluna 'personagem' com '--'
#df = df.fillna('--', subset=['personagem'])

### FAZER PESQUISA HARRY
palavras_chave_h = ['Harry Potter']

# Filtrar filmes relacionados ao Harry Potter
resultados_hp = df.filter(col('personagem').rlike('|'.join(palavras_chave_h)))

### FAZER PESQUISA JOGOS VORAZES
palavras_chave_hg = ['Katniss Everdeen']

# Filtrar filmes relacionados aos Jogos Vorazes
resultados_hg = df.filter(col('personagem').rlike('|'.join(palavras_chave_hg)))

### FAZER PESQUISA CREPUSCULO
palavras_chave_c = ['Bella Swan']

# Filtrar filmes relacionados a Crepúsculo
resultados_tw = df.filter(col('personagem').rlike('|'.join(palavras_chave_c)))

### JUNTAR TUDO
df_sagas = resultados_hp.union(resultados_hg).union(resultados_tw)

# Lista de IDs para excluir
ids_para_excluir = ['tt0092115', 'tt20766450', 'tt8443702']

# Filtrar DataFrame para manter apenas as linhas desejadas
df_sagas = df_sagas.filter(~col('id').isin(ids_para_excluir))


df_sagas = df_sagas.withColumn('anoLancamento', df_sagas['anoLancamento'].cast('float'))


print(df_sagas)

#############################################
# FAZER DF GERAL DO CSV PELO GÊNERO
generos_procurados = ['Fantasy', 'Sci-Fi']

# Preencher valores NaN nas colunas 'genero' e 'anoLancamento'
df = df.fillna({'genero': '', 'anoLancamento': 0})

# Converter a coluna 'anoLancamento' para numérica
df = df.withColumn('anoLancamento', df['anoLancamento'].cast('int'))

# Filtrar filmes com base nos critérios especificados
df_csv_genero = df.filter((col('genero').rlike('|'.join(generos_procurados))) & (col('anoLancamento').between(2001, 2015)))

# Remover linhas duplicadas com base no ID
df_csv_genero = df_csv_genero.dropDuplicates(['id'])

# Preencher valores NaN na coluna 'tempoMinutos' com 0
df_csv_genero = df_csv_genero.fillna(0, subset=['tempoMinutos'])

# Converter a coluna 'tempoMinutos' para inteiro e remover linhas com valores menores que 100
df_csv_genero = df_csv_genero.withColumn('tempoMinutos', df_csv_genero['tempoMinutos'].cast('int'))
df_csv_genero = df_csv_genero.filter(df_csv_genero['tempoMinutos'] >= 80)

# Remover linhas com 'numeroVotos' menores que 30
df_csv_genero = df_csv_genero.filter(df_csv_genero['numeroVotos'] >= 30)

print(df_csv_genero)

################################ JUNTAR TUDO
# Unir os DataFrames
df_final = df_sagas.union(df_csv_genero)

# Remover linhas duplicadas com base no ID
df_final = df_final.dropDuplicates(['id'])

# Remover colunas desnecessárias
colunas_para_excluir = ['genero', 'tituloOriginal', 'anoLancamento', 'generoArtista', 'personagem', 'nomeArtista', 'anoNascimento', 'anoFalecimento', 'profissao', 'titulosMaisConhecidos']
df_final = df_final.drop(*colunas_para_excluir)

# Renomear a coluna 'tituloPincipal' para 'tituloPrincipal'
df_final = df_final.withColumnRenamed('tituloPincipal', 'tituloPrincipal') \
                   .withColumnRenamed('id', 'imdb_id')\
                   .withColumnRenamed('notaMedia', 'notaMedia_imdb')\
                   .withColumnRenamed('numeroVotos', 'numeroVotos_imdb') 
                   
                   
print(df_final)

### SALVANDO OS DF
df_final=df_final.coalesce(1)
df_final.write.parquet(output_path_final, mode='overwrite')



job.commit()