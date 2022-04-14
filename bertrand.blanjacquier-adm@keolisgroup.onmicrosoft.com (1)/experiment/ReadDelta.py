# Databricks notebook source
def unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)


configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "fe7690dd-5449-46c6-84f0-8aad8f1cb4f8",
       "fs.azure.account.oauth2.client.secret": "6Fk7Q~8Vwfy6FUMjBfky3G5DmgrNhWzfY.m.S",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5ee48532-0e3a-4785-9f41-9b20f1aaf4f7/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

unmount("/mnt/raw")
unmount("/mnt/curated")
        
dbutils.fs.mount(source = "abfss://raw@bbladls.dfs.core.windows.net",mount_point = "/mnt/raw",extra_configs = configs)
dbutils.fs.mount(source = "abfss://curated@bbladls.dfs.core.windows.net",mount_point = "/mnt/curated",extra_configs = configs)

rawCard = spark.read.parquet("mnt/raw/TransCity-DWH/CARD/*/*/*/CARD")
#display(rawCard)

write_format = 'delta'
write_mode = 'overwrite'
#partition_by = 'CARDTYPEID'
save_path = '/mnt/curated/TransCity-DWH/Card'
 
# Write the data to its target.
"""
rawCard.write \
  .format(write_format) \
  .partitionBy(partition_by) \
  .mode(write_mode) \
  .save(save_path)
"""
rawCard.write \
  .format(write_format) \
  .mode(write_mode) \
  .save(save_path)

table_name = 'Card'
display(spark.sql("DROP TABLE IF EXISTS " + table_name))
display(spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'"))

display(spark.sql("DESCRIBE FORMATTED " + table_name))


# COMMAND ----------

spark.sql("DROP TABLE " + table_name)
# Delete the Delta files.
dbutils.fs.rm(save_path, True)
