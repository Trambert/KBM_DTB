# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

def keyExpression(key):
    tabKey = key.split("|")
    expression =""
    for id in tabKey:
        expression += "old."+id+" = updates."+id+" and "
    return "".join(expression.rsplit(" and ", 1))

def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return True
    else:
        return False

    
def createOrUpdateDelta(newDf, save_path, key, deltaDB, object):
    #Check if Delta table exists
    isDelta = DeltaTable.isDeltaTable(spark, save_path)
    if isDelta:
        deltaTable = DeltaTable.forPath(spark, save_path)
        deltaTable.alias("old").merge(
            source = newDf.alias("updates"),
            condition = expr(keyExpression(key))
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        table_name = object
        newData.write.format('delta').mode('overwrite').save(save_path)
        display(spark.sql("DROP TABLE IF EXISTS " + deltaDB + "." + table_name))
        display(spark.sql("CREATE TABLE " + deltaDB + "." + table_name + " USING DELTA LOCATION '" + save_path + "'"))

def createOrUpdateDeltaWithPartition(newDf, save_path, key, deltaDB, object, partitionBy):
    #Check if Delta table exists
    isDelta = DeltaTable.isDeltaTable(spark, save_path)
    if isDelta:
        deltaTable = DeltaTable.forPath(spark, save_path)
        deltaTable.alias("old").merge(
            source = newDf.alias("updates"),
            condition = expr(keyExpression(key))
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        table_name = object
        newData.write.partitionBy(partitionBy).format('delta').mode('overwrite').save(save_path)
        display(spark.sql("DROP TABLE IF EXISTS " + deltaDB + "." + table_name))
        display(spark.sql("CREATE TABLE " + deltaDB + "." + table_name + " USING DELTA LOCATION '" + save_path + "'"))
    
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

if not chkmount("/mnt/reload"):
    dbutils.fs.mount(source = "abfss://reload@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/reload",extra_configs = configs)

Source = dbutils.widgets.get("Source")
Object = dbutils.widgets.get("Object")
TableKey = dbutils.widgets.get("TableKey")
LastLoadDate = dbutils.widgets.get("LastLoadDate")
PartitionBy = dbutils.widgets.get("PartitionBy")
DeltaDatabase = dbutils.widgets.get("DeltaDatabase")
print(LastLoadDate)
year,month,day=LastLoadDate.split('-')
    

save_path = '/mnt/curated/'+Source+'/'+Object
#Read new data
newData = spark.read.parquet(f"mnt/reload/{Source}/{Object}/{year}/{month}/{day}/{Object}*.parquet")
print(newData.count())
#if(len(PartitionBy)>0):
    #createOrUpdateDeltaWithPartition(newData, save_path, TableKey, DeltaDatabase,Object, PartitionBy)
#else:
    #createOrUpdateDelta(newData, save_path, TableKey, DeltaDatabase,Object)
    

# COMMAND ----------

def lsR(path):
  return([fname for flist in [([fi.path] if fi.isFile() else lsR(fi.path)) for fi in dbutils.fs.ls(path)] for fname in flist])
                
#files = lsR('mnt/raw/EVEX/activite')

#for fi in files: 
  #print(fi)
