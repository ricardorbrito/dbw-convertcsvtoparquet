# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
# MAGIC %python
# MAGIC # Estabelece a conexao com o datatalake com configuração de secrets/k-vault 
# MAGIC config = {"fs.azure.acount.key.dlscognitivo.blob.core.windows.net/":dbutils.secrets.get(scope = "scp-kv-cognitivo", key = "secret-key-datalake")}

# COMMAND ----------

# DBTITLE 1,Lista de containers
# MAGIC %python
# MAGIC #cria um array para listar as camamadas de transições dos arquivos.
# MAGIC containers = ["raw","bronze"]

# COMMAND ----------

# DBTITLE 1,Criar ponto de montagem nas camadas
#cria leitura do array para listar as camamadas de transições dos arquivos.
#caso seja adicionada ooutra camanda no datalake não precisará mexer aqui, apenas adicinar no array containers
def mount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.mount(
                source = f"abfss://{container}@dlscognitivo.blob.core.windows.net/",
                mount_point = f"/mnt/{container}",
                extra_configs = config
            )
            print(container)
    except ValueError as err:
        print(err)
        

# COMMAND ----------

# DBTITLE 1,Desmonta camadas do DBFS - Alerta (Apaga camadas)
#caso precise quebrar a conexao com as camadas

def unmount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.unmount(f"/mnt/{container}/")
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Requisito 01 - Conversão do formato dos arquivos: 
# MAGIC %python
# MAGIC #Converter o arquivo CSV presente no diretório data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;
# MAGIC #Para este desafio, escolhoe Parquet é uma estrutura de armazenamento de alto desempenho e muito eficiente. o Parquet permite que qualquer projeto no ecossistema Hadoop use uma representação 
# MAGIC #de dados colunar compactada e eficiente . Ele suporta formatos de compactação: Snappy, GZIP, Lzo
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from delta.tables import *
# MAGIC from pandas import *
# MAGIC 
# MAGIC 
# MAGIC #Diretorio de origem
# MAGIC raw_path_load = "/mnt/raw/load.csv"
# MAGIC 
# MAGIC #Diretorio de destino
# MAGIC deltaTable = "bronze.loadconvertida"
# MAGIC bronze_path_parquetFiles = "/mnt/bronze/"
# MAGIC  
# MAGIC #Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
# MAGIC spark.conf.set('var.raw_path_load', raw_path_load)
# MAGIC spark.conf.set('var.path_deltaTable', deltaTable)
# MAGIC spark.conf.set('var.bronze_path_parquetFiles', bronze_path_parquetFiles)
# MAGIC 
# MAGIC loadDF = spark.read.csv(raw_path_load, header=True, sep=',', inferSchema=False)
# MAGIC #loadDF.createOrReplaceTempView("tempView_loadPython")

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
# MAGIC %python
# MAGIC loadDF.show()
# MAGIC #ou
# MAGIC display(loadDF)

# COMMAND ----------

# DBTITLE 1,Criando o arquivo parquet na camada bronze
loadDF.write.parquet(bronze_path_parquetFiles+"/loadConvertida")


# COMMAND ----------

# DBTITLE 1,Lendo o arquivo parquet em um dataframfe
parquetDF=spark.read.parquet(bronze_path_parquetFiles+"/loadConvertida")
display(parquetDF)

# COMMAND ----------

# DBTITLE 1,Requisito - 02 - Desduplicação dos dados convertidos:
#Removendo linhas duplicadas com base no ID e preservando o registro com base no update_date mais recente- PYTHON
import pyspark
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import json

display(parquetDF.orderBy(col("update_date").desc()).dropDuplicates(["id"]))

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo de configuração de METADADOS (types_mapping.json) e Atualização dos dados
# MAGIC %python
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC from pyspark import SparkConf, SparkContext
# MAGIC from pyspark.sql.session import SparkSession
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import dense_rank , rank, col, max
# MAGIC import json
# MAGIC 
# MAGIC file_path = "mnt/raw/types_mapping.json"
# MAGIC 
# MAGIC df_json = spark.read.json(file_path,multiLine=True)
# MAGIC df_json.display()
# MAGIC df_final = df_json
# MAGIC with open(file_path) as json_file:  
# MAGIC 	    data = json.load(json_file)
# MAGIC 	    for d in data:
# MAGIC 	    	# Modificando Schema
# MAGIC 	        df_final = df_final.withColumn(d,col(d).cast(data[d]))
# MAGIC df_final..display()
# MAGIC    
# MAGIC      

# COMMAND ----------


