# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pandas import *


# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
raw_path_load = "/mnt/raw/load.csv"

#Diretorio de destino
deltaTable = "bronze.loadconvertida"

bronze_path_parquetFiles = "/mnt/bronze/"
 
#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL

spark.conf.set('var.raw_path_load', raw_path_load)

spark.conf.set('var.path_deltaTable', deltaTable)

spark.conf.set('var.bronze_path_parquetFiles', bronze_path_parquetFiles)

# COMMAND ----------

# DBTITLE 1,Leitura load csv - SQL
# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_loadSQL USING csv
# MAGIC 
# MAGIC OPTIONS ('header' = 'true', 'inferSchema' = 'false', sep=',', path '${var.raw_path_load}')

# COMMAND ----------

# DBTITLE 1,Leitura load csv- Python
loadDF = spark.read.csv(raw_path_load, header=True, sep=',', inferSchema=False)
loadDF.createOrReplaceTempView("tempView_loadPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
loadDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_loadSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
loadDF.show()
#ou
display(loadDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
tempView_artistaPythonDF = spark.table("tempView_loadSQL")
#ou
display(loadDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - SQL
# MAGIC %sql
# MAGIC  SELECT id
# MAGIC         ,name
# MAGIC         ,email
# MAGIC         ,phone
# MAGIC         ,address
# MAGIC         ,age
# MAGIC         ,create_Date
# MAGIC         ,update_date 
# MAGIC    FROM tempView_loadSQL

# COMMAND ----------

# DBTITLE 1,Removendo linhas duplicadas com base no ID e preservando o registro com base no update_date mais recente- PYTHON
from pyspark.sql.functions import col
display(loadDF.orderBy(col("update_date").desc()).dropDuplicates(["id"]))


