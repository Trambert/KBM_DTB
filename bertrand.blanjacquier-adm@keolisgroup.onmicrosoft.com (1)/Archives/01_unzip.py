# Databricks notebook source
# Connection to ADLS parameters
adls_acc_name = 'kbmkmaeuwdls'
sp_client_id = dbutils.secrets.get(scope = "p-kbm-kma-euw-kv", key = "p-kbm-spadb2adls-clientid")
sp_tenant_id = dbutils.secrets.get(scope = "p-kbm-kma-euw-kv", key = "p-kbm-spadb2adls-tenantid")
sp_key = dbutils.secrets.get(scope = "p-kbm-kma-euw-kv", key = "p-kbm-spadb2adls-secret")

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "{}".format(sp_client_id),
  "fs.azure.account.oauth2.client.secret": "{}".format(sp_key),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(sp_tenant_id),
  "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# COMMAND ----------

# Load parameters from Data Factory
file_system = dbutils.widgets.get("file_system")
path = dbutils.widgets.get("path")
filename = dbutils.widgets.get("filename")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

# Mount
from pathlib import Path

mp = "/mnt/zipped/{}".format(run_id)
dbutils.fs.mount(
  source = "abfss://{}@{}.dfs.core.windows.net/{}".format(file_system, adls_acc_name, path),
  mount_point = mp,
  extra_configs = configs)


# COMMAND ----------

# Show files
display(dbutils.fs.ls(mp))


# COMMAND ----------

# Remove previous files if they exist
import shutil

dirpath = Path("/dbfs/mnt/zipped", run_id, filename)
if dirpath.exists() and dirpath.is_dir():
    print('Remove ' + str(dirpath))
    shutil.rmtree(dirpath)

# COMMAND ----------

# Unzip in subfolder
import py7zr

filepath = Path("/dbfs/mnt/zipped", run_id, filename + '.7z')
with py7zr.SevenZipFile(filepath,"r") as zip_ref:
    zip_ref.extractall(dirpath)

# COMMAND ----------

# Show new files
display(dbutils.fs.ls(mp + "/{}".format(filename)))

# COMMAND ----------

# Unmount
dbutils.fs.unmount(mp)
