# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
# MAGIC %python
# MAGIC config = {"fs.azure.account.key.dlsturma05imersaoprod.blob.core.windows.net":dbutils.secrets.get(scope = "scp-kv-prod", key = "secret-key-datalake")}

# COMMAND ----------

# DBTITLE 1,Lista de containers
# MAGIC %python
# MAGIC containers = ["bronze", "silver", "gold"]

# COMMAND ----------

# DBTITLE 1,Criar ponto de montagem nas camadas
def mount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.mount(
                source = f"wasbs://{container}@dlsturma05imersaoprod.blob.core.windows.net",
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
# MAGIC ls /mnt/bronze/csv

# COMMAND ----------

# MAGIC %python
# MAGIC mount_datalake(containers)

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
# MAGIC keydatalake = dbutils.secrets.get(scope = "scp-kv-prod", key = "secret-key-datalake")
# MAGIC print("teste/"+keydatalake)
