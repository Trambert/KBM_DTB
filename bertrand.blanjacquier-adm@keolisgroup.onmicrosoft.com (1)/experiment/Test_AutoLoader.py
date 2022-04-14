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


Source = "TransCity-DWH"
Object = "CARD"
TableKey = "CARDID"
tableSchema = ""
    
    
#raw = spark.read.parquet("mnt/raw/TransCity-DWH/CARD/*/*/*/CARD*.parquet")
#raw = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
#display(raw)

#write_format = 'delta'
#write_mode = 'overwrite'
#partition_by = 'CARDTYPEID'
save_path = '/mnt/curated/'+Source+'/'+Object
print(save_path)
#raw.write.format(write_format).mode(write_mode).save(save_path)

newData = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
#Check if Delta table exists
isDelta = DeltaTable.isDeltaTable(spark, save_path)
if isDelta:
    deltaTable = DeltaTable.forPath(spark, save_path)
    deltaTable.alias("old").merge(
        newData.alias("updates"),
        'old.'+TableKey+' = updates.'+TableKey
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    newData.write.format('delta').mode('overwrite').save(save_path)

#table_name = Object
#display(spark.sql("DROP TABLE IF EXISTS " + table_name))
#display(spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'"))

#display(spark.sql("DESCRIBE FORMATTED " + table_name))

# COMMAND ----------

newData = spark.read.parquet("mnt/raw/TransCity-DWH/CARD/2022/01/28/*.parquet")
save_path = '/mnt/curated/TransCity-DWH/CARD'
newData.write.format('delta').mode('overwrite').save(save_path)
display(spark.sql("CREATE TABLE CARD USING DELTA LOCATION '" + save_path + "'"))


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE CARD

# COMMAND ----------

save_path = '/mnt/curated/TransCity-DWH/CARD'
deltaTable = DeltaTable.forPath(spark, save_path)
newData = spark.read.parquet("/mnt/raw/TransCity-DWH/2022/01/29/CARD_*.parquet")
TableKey = 'CARDID'

deltaTable.alias("old").merge(
    newData.alias("updates"),
    'old.'+TableKey+' = updates.'+TableKey
  ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

Source = 'TransCity-DWH'
Object = 'CARD'
TableKey = 'CARDID'
isIncremental = True

newData = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
schema = newData.schema

dbutils.fs.rm(f"/mnt/raw/schema/{Object}.json", True)
with open(f"/dbfs/mnt/raw/schema/{Object}.json", "w") as f:
  f.write(newData.schema.json())


# COMMAND ----------

from pyspark.sql.streaming import *
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles",1)

#Source = dbutils.widgets.get("Source")
#Object = dbutils.widgets.get("Object")
#TableKey = dbutils.widgets.get("TableKey")
#isIncremental = dbutils.widgets.get("isIncremental")
global key
key = 'CARDID'

Source = 'TransCity-DWH'
Object = 'CARD'
TableKey = 'CARDID'
isIncremental = True

newData = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
schema = newData.printSchema()


# Checkpoint folder to use by the autoloader in order to store streaming metatdata
#schemalocation = f"/mnt/raw/_checkpoint/{Source}/{Object}/"
schemalocation = '/mnt/raw/schema/"
autoLoaderSrcPath = f"/mnt/raw/{Source}/{Object}/"
#"mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet"
# Destination delta table path
deltaPath = f"/mnt/curated/{Source}/{Object}"

cloudFile = {
    "cloudFiles.format":"parquet"
    ,"cloudFiles.inferColumnTypes":"true"
    ,"cloudFiles.schemaLocation":f"{schemalocation}"
}



df = (spark \
      .readStream \
      .format("cloudFiles") \
      .options(**cloudFile) \
      .option("maxFilesPerTrigger", 1) \
      .option("rescuedDataColumn","_rescued_data") \
      .load(autoLoaderSrcPath))


df.writeStream \
  .format("delta") \
  .queryName(f"AutoLoad_{Object}") \
  .trigger(once=True) \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .start()

def upsertToDelta(microBatchOutputDF, batchId): 
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # ==============================
  # Supported in DBR 5.5 and above
  # ==============================

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  microBatchOutputDF._jdf.sparkSession().sql("""
    MERGE INTO {Object} t
    USING updates s
    ON s.{key} = t.{key}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

