# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

def upsertToDeltaFACT_VALIDATION(microBatchOutputDF, batchId): 
  # Set the dataframe to view name
    updates = microBatchOutputDF.dropDuplicates(subset = ['VALIDATIONID'])
    updates.createOrReplaceTempView("updates")
    #updates = spark.sql("select * from updates")
    
    isDelta = DeltaTable.isDeltaTable(spark, '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION')
    if isDelta:
        deltaTable = DeltaTable.forPath(spark, '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION')
        deltaTable.alias("old").merge(
            source = updates.alias("updates"),
            condition = expr("old.VALIDATIONID = updates.VALIDATIONID")
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        table_name = object
        updates.write.format('delta').mode('overwrite').save('/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION')
        display(spark.sql("DROP TABLE IF EXISTS autoloader.FACT_VALIDATION"))
        display(spark.sql("CREATE TABLE autoloader.FACT_VALIDATION USING DELTA LOCATION '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION'"))

#spark.conf.set("spark.sql.parquet.enableVectorizedReader","true")
checkpoint_path = '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION/_checkpoints'
write_path = '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION'
upload_path = '/mnt/raw/TransCity-DWH/FACT_VALIDATION/*/*/*'

# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'parquet') \
  .schema('VALIDATIONID	decimal(19,0), \
    SESSIONID	decimal(19,0), \
    DEVICEID	decimal(19,0), \
    SESSIONVEHICLEID	decimal(19,0), \
    COMPANYID	decimal(19,0), \
    TCN	string, \
    FAREPRODUCTITEMID	decimal(19,0), \
    PRODUCTID	decimal(19,0), \
    PRODUCTFAMILYID	decimal(19,0), \
    PRODUCTISSUERID	decimal(19,0), \
    OPERATORID	decimal(19,0), \
    TRIPCOUNTERBEFOREVALIDATION	int, \
    TRIPCOUNTERAFTERVALIDATION	int, \
    VALIDATIONNUMBER	decimal(19,0), \
    VEHICLEID	decimal(19,0), \
    VEHICLETYPEID	decimal(19,0), \
    LINEID	decimal(19,0), \
    ITINERARYID	decimal(19,0), \
    DIRECTION	int, \
    FARESTOPID	decimal(19,0), \
    VALIDATIONDATE	timestamp, \
    CARDID	decimal(19,0), \
    TICKETID	decimal(19,0), \
    VALIDATIONSTATUSID	int, \
    FAILUREREASONID	decimal(19,0), \
    CONTRACTVALIDITYENDDATE	timestamp, \
    VALIDATIONSTATEID	int, \
    PRODUCTSTARTVALIDITY	timestamp, \
    PRODUCTENDOFVALIDITY	timestamp, \
    RV	string, \
    HOURID	string, \
    TIMESTEPHOUR	string, \
    TIMESTEP30MIN	string, \
    TIMESTEP15MIN	string, \
    VALIDATIONDATEID	int, \
    STOPINDEX	int') \
    .option('maxFilePerTrigger', 1) \
    .load(upload_path)

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that
# have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check,
# write the newly-uploaded files' data to the write_path location.
df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .foreachBatch(upsertToDeltaFACT_VALIDATION) \
  .start(write_path)

#.trigger(once=True) \

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC --create database autoloader;
# MAGIC CREATE TABLE autoloader.FACT_VALIDATION
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/raw/delta/TransCity-DWH/FACT_VALIDATION'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM autoloader.FACT_VALIDATION  LIMIT 1000;

# COMMAND ----------

#spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table autoloader.FACT_VALIDATION
# MAGIC --15211966
# MAGIC --15211966
# MAGIC 
# MAGIC --select count(*), validationid from autoloader.fact_validation
# MAGIC --group by validationid
# MAGIC --HAVING COUNT(*) > 1
# MAGIC select count(*) from autoloader.fact_validation;
# MAGIC --select count(*) from default.fact_validation;
