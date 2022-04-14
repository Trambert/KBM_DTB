# Databricks notebook source
jdbcPassword = dbutils.secrets.get(scope = "kec-kdp-euw-adb-kvt", key = "kec-kdp-cnx-pwd")
jdbcUsername = 'srvadmin@dkeckdpeuwpos'
jdbcHostname = 'dkeckdpeuwpos.postgres.database.azure.com'
jdbcPort = '5432'
jdbcDatabase = 'postgres'

# COMMAND ----------

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://dkeckdpeuwpos.postgres.database.azure.com:5432/postgres"
table = "merge_all_provider"

# COMMAND ----------

remote_table = spark.read.format("jdbc") \
                          .option("driver", driver) \
                          .option("url", url) \
                          .option("dbtable", table) \
                          .option("user", jdbcUsername) \
                          .option("password", jdbcPassword) \
                          .option("ssl", True) \
                          .load()
display(remote_table)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

df_parquet = spark.read.option("mergeSchema", "true").option("basePath", "/mnt/lab/merge-all-provider/merge-all-provider-France.parquet/").parquet('/mnt/lab/merge-all-provider/merge-all-provider-France.parquet/year=2021/month=1/day=28/providerId=*/date_process=*/')

# COMMAND ----------

df_parquet1 = spark.read.option("mergeSchema", "true").option("basePath", "/mnt/lab/merge-all-provider/merge-all-provider-France.parquet/").parquet('/mnt/lab/merge-all-provider/merge-all-provider-France.parquet/year=2021/month=1/day=28/providerId=Madvertise/date_process=*/')

# COMMAND ----------

from pyspark.sql.functions import lit,col, when, abs

df_parquet_puissance1 = df_parquet1.where(col('providerId') == 'Madvertise').withColumn("longitude", when(abs(col('longitude')) >214, col('longitude')).otherwise(col('longitude') * 10000000)) \
                                   .withColumn("latitude", when(abs(col('latitude')) >214, col('latitude')).otherwise(col('latitude') * 10000000))
      

display(df_parquet_puissance1)

# COMMAND ----------

from pyspark.sql.functions import lit,col, when, abs

df_parquet_puissance = df_parquet.where(col('providerId') == 'Madvertise').withColumn("longitude", when(abs(col('longitude')) >214, col('longitude')).otherwise(col('longitude') * 10000000)) \
                                  .withColumn("latitude", when(abs(col('latitude')) >214, col('latitude')).otherwise(col('latitude') * 10000000)).limit(10)
      

display(df_parquet_puissance)

# COMMAND ----------

df_parquet_puissance = df_parquet.withColumn("longitude", NULL)) \
                                  .withColumn("latitude", when(abs(col('latitude')) >214, col('latitude')).otherwise(col('latitude') * 10000000)).limit(10)

CREATE TABLE public.test("")
(
    "uuid" uuid not NULL,
    longitude double precision DEFAULT NULL,
    latitude double precision DEFAULT NULL,
    "timestamp" timestamp without time zone,
    speed double precision DEFAULT NULL,
    os text DEFAULT NULL,
    horizontal_accuracy double precision DEFAULT NULL,
    country text DEFAULT NULL,
    altitude double precision DEFAULT NULL,
    vertical_accuracy double precision DEFAULT NULL,
    os_version text DEFAULT NULL,
    year text not NULL,
    month text not NULL,
    day text not NULL,
    providerId text not NULL,
    date_process date not NULL

) PARTITION BY RANGE (year, month, day);


# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

df_parquet_puissance1.write.format("jdbc")\
                      .option("stringtype", "unspecified") \
                      .option("driver", driver)\
                      .option("url", url)\
                      .option("dbtable", table)\
                      .option("user", jdbcUsername)\
                      .option("password", jdbcPassword)\
                      .option("ssl", True) \
                      .mode("append") \
                      .save()

# COMMAND ----------


