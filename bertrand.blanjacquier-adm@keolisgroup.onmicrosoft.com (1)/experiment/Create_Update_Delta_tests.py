# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return True
    else:
        return False

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "9c3d7661-59c0-418c-8b0e-684c1b745edf",
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "key-vault-secrets", key = "DtbSecretId"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7124e463-2734-41bf-bddb-3e475374f94c/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

if not chkmount("/mnt/raw"):
    dbutils.fs.mount(source = "abfss://raw@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/raw",extra_configs = configs)
    
if not chkmount("/mnt/curated"):
    dbutils.fs.mount(source = "abfss://curated@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/curated",extra_configs = configs)


Source = dbutils.widgets.get("Source")
Object = dbutils.widgets.get("Object")
TableKey = dbutils.widgets.get("TableKey")
LastLoadDate = dbutils.widgets.get("LastLoadDate")
year,month,day=LastLoadDate.split('-')
    
#raw = spark.read.parquet("mnt/raw/TransCity-DWH/CARD/*/*/*/CARD*.parquet")
#raw = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
#display(raw)

#write_format = 'delta'
#write_mode = 'overwrite'
#partition_by = 'CARDTYPEID'
save_path = '/mnt/curated/'+Source+'/'+Object
#print(save_path)
#raw.write.format(write_format).mode(write_mode).save(save_path)

#newData = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
newData = spark.read.parquet(f"mnt/raw/{Source}/{Object}/{year}/{month}/{day}/{Object}*.parquet")


#Check if Delta table exists
isDelta = DeltaTable.isDeltaTable(spark, save_path)
if isDelta:
    deltaTable = DeltaTable.forPath(spark, save_path)
    deltaTable.alias("old").merge(
        source = newData.alias("updates"),
        condition = expr("old."+TableKey+" = updates."+TableKey)
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    newData.write.format('delta').mode('overwrite').save(save_path)

table_name = Object
display(spark.sql("DROP TABLE IF EXISTS " + table_name))
display(spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'"))

#display(spark.sql("DESCRIBE FORMATTED " + table_name))

# COMMAND ----------

#dbfs:/mnt/raw/EVEX/activite/2022/02/10/activite*.parquet

from delta.tables import *
from pyspark.sql.functions import *

def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return True
    else:
        return False

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "9c3d7661-59c0-418c-8b0e-684c1b745edf",
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "key-vault-secrets", key = "DtbSecretId"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7124e463-2734-41bf-bddb-3e475374f94c/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

#if not chkmount("/mnt/raw"):
#dbutils.fs.unmount("/mnt/raw")
#dbutils.fs.mount(source = "abfss://raw@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/raw",extra_configs = configs)

    
#print(chkmount("/mnt/raw"))        
newData = spark.read.parquet(f"mnt/raw/EVEX/activite/2022/02/10/activite_*.parquet")
display(newData)

# COMMAND ----------


"""
def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        True
    else:
        False


configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "fe7690dd-5449-46c6-84f0-8aad8f1cb4f8",
       "fs.azure.account.oauth2.client.secret": "6Fk7Q~8Vwfy6FUMjBfky3G5DmgrNhWzfY.m.S",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5ee48532-0e3a-4785-9f41-9b20f1aaf4f7/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "key-vault-secrets", key = "TBM-DL-KEY")}

if not chkmount("/mnt/raw"):
    dbutils.fs.mount(source = "abfss://raw@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/raw")

    
if not chkmount("/mnt/curated"):
    dbutils.fs.mount(source = "abfss://curated@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/curated")
        

rawCard = spark.read.parquet("mnt/raw/TransCity-DWH/CARD/*/*/*/CARD")
#display(rawCard)

write_format = 'delta'
write_mode = 'overwrite'
#partition_by = 'CARDTYPEID'
save_path = '/mnt/curated/TransCity-DWH/Card'
""" 
# Write the data to its target.
"""
rawCard.write \
  .format(write_format) \
  .partitionBy(partition_by) \
  .mode(write_mode) \
  .save(save_path)

rawCard.write \
  .format(write_format) \
  .mode(write_mode) \
  .save(save_path)

table_name = 'Card'
display(spark.sql("DROP TABLE IF EXISTS " + table_name))
display(spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'"))

display(spark.sql("DESCRIBE FORMATTED " + table_name))
"""

# COMMAND ----------

Source = 'EVEX'
Object = 'activite'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'agent_de_maitrise'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'consequence'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'direction'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'evenement'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'ligne'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'motif'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'arret_evw'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))


Source = 'EVEX'
Object = 'arret_lieu_evw'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'ligne_evw'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'regroupement_voyages_comptables'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'table_produit'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Source = 'EVEX'
Object = 'validation'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

# COMMAND ----------

Source = 'TransCity-DWH'

Object = 'CARD'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))


Object = 'CARDTYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))


Object = 'DEVICE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'DEVICETYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'DIM_PRODUCT'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'FACT_VALIDATION'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'ITINERARY'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'LINE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'PRODUCT'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'STOP'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'TICKET'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'VALIDATIONREJECTREASON'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'VEHICLE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

Object = 'VEHICLETYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))

# COMMAND ----------

Source = 'TransCity-DWH'

Object = 'CARD'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("DROP TABLE IF EXISTS " + Object))
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'CARDTYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))


Object = 'DEVICE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'DEVICETYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'DIM_PRODUCT'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'FACT_VALIDATION'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'ITINERARY'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'LINE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'PRODUCT'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'STOP'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'TICKET'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'VALIDATIONREJECTREASON'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'VEHICLE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))

Object = 'VEHICLETYPE'
save_path = '/mnt/curated/'+Source+'/'+Object
display(spark.sql("CREATE TABLE " + Object + " USING DELTA LOCATION '" + save_path + "'"))
