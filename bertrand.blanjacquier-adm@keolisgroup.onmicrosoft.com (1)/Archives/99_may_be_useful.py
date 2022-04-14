# Databricks notebook source
adls_acc_name = 'kbmkmaeuwdls'
file_system = 'developpement'
path = ''

# COMMAND ----------

#Méthode simple d'accès
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

# Mount
from pathlib import Path

mp = "/mnt/zipped/01"
dbutils.fs.mount(
  source = "abfss://{}@{}.dfs.core.windows.net/{}".format(file_system, adls_acc_name, path),
  mount_point = mp,
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(mp))

# COMMAND ----------


