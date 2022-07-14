# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from delta.tables import *
# MAGIC from pandas import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
# MAGIC %python
# MAGIC #Diretorio de origem
# MAGIC raw_path_load = "/mnt/raw/load.csv"
# MAGIC 
# MAGIC #Diretorio de destino
# MAGIC deltaTable = "bronze.loadconvertida"
# MAGIC 
# MAGIC bronze_path_parquetFiles = "/mnt/bronze/"
# MAGIC  
# MAGIC #Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
# MAGIC 
# MAGIC spark.conf.set('var.raw_path_load', raw_path_load)
# MAGIC 
# MAGIC spark.conf.set('var.path_deltaTable', deltaTable)
# MAGIC 
# MAGIC spark.conf.set('var.bronze_path_parquetFiles', bronze_path_parquetFiles)

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
# MAGIC %python
# MAGIC loadDF = spark.read.csv(raw_path_load, header=True, sep=',', inferSchema=False)
# MAGIC loadDF.createOrReplaceTempView("tempView_loadPython")

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
# MAGIC %python
# MAGIC loadDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tempView_loadSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
# MAGIC %python
# MAGIC loadDF.show()
# MAGIC #ou
# MAGIC display(loadDF)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
# MAGIC %python
# MAGIC tempView_artistaPythonDF = spark.table("tempView_loadSQL")
# MAGIC #ou
# MAGIC display(loadDF)

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
# MAGIC %python
# MAGIC 
# MAGIC import pyspark
# MAGIC from pyspark.sql.functions import explode
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.types import StructType
# MAGIC import json
# MAGIC 
# MAGIC display(loadDF.orderBy(col("update_date").desc()).dropDuplicates(["id"]))

# COMMAND ----------

# DBTITLE 1,Lendo o Json - PYTHON
# MAGIC %python
# MAGIC import json
# MAGIC file_path = "/mnt/raw/types_mapping.json"
# MAGIC df_json = spark.read.json(file_path,multiLine=True)
# MAGIC display(df_json)

# COMMAND ----------

# DBTITLE 1,Transformando o Json em Parquet
# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
lines = sc.textFile("/mnt/raw/types_mapping.json")
parts = lines.map(lambda l: l.split(","))
tabela = parts.map(lambda p: (p[0], p[1].strip()))


# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]

schema = StructType(fields)
print (schema)
# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(tabela, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("tabela")

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT * FROM tabela")


    
     
    
    
    

    
   




# COMMAND ----------

# DBTITLE 1,Varrendo o arquivo parquet
# sc is an existing SparkContext.
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
people = sqlContext.read.json("/mnt/raw/types_mapping.json")

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame.
people.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by `sqlContext`.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
anotherPeopleRDD = sc.parallelize([
  '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
