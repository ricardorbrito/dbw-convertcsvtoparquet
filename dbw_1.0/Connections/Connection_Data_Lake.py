# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
# MAGIC %python
# MAGIC config = {"fs.azure.acount.key.dlscognitivo.blob.core.windows.net/":dbutils.secrets.get(scope = "scp-kv-cognitivo", key = "secret-key-datalake")}

# COMMAND ----------

# DBTITLE 1,Lista de containers
# MAGIC %python
# MAGIC containers = ["raw"]

# COMMAND ----------

# DBTITLE 1,Criar ponto de montagem nas camadas
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
def unmount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.unmount(f"/mnt/{container}/")
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
# MAGIC %fs
# MAGIC 
# MAGIC ls 

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt")

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
# MAGIC %python
# MAGIC dbutils.fs.ls("/mnt/")

# COMMAND ----------

# DBTITLE 1,Validação Key Vault
# MAGIC %python
# MAGIC keydatalake = "HJHthyuiop245#$%¨&*()_"
# MAGIC print(keydatalake)
# MAGIC 
# MAGIC keydatalake = dbutils.secrets.get(scope = "scp-kv-cognitivo", key = "secret-key-datalake")
# MAGIC print("teste/"+keydatalake)
