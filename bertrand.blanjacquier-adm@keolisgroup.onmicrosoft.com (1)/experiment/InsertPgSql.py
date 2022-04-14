# Databricks notebook source
#jdbcPassword = dbutils.secrets.get(scope = "kec-kdp-euw-adb-kvt", key = "kec-kdp-cnx-pwd")
jdbcUsername = 'admintbm@dkbmtbmeuwpgdtb'
jdbcHostname = 'dkbmtbmeuwpgdtb.postgres.database.azure.com'
jdbcPort = '5432'
jdbcDatabase = 'picj5'

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5"


spark.table("bi.validation").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
    .option("dbtable", "public.validation") \
    .option("user", "admintbm@dkbmtbmeuwpgdtb") \
    .option("password", "C_9aVu?qDAia!Rm") \
    .option("ssl", True) \
    .option("sslmode", "require") \
    .option("truncate", "true").mode("overwrite") \
    .save()


#spark.table("bi.validation").write.jdbc(jdbcUrl, "diamonds", connectionProperties)

# COMMAND ----------

#jdbcPassword = dbutils.secrets.get(scope = "kec-kdp-euw-adb-kvt", key = "kec-kdp-cnx-pwd")
jdbcUsername = 'admintbm@dkbmtbmeuwpgdtb'
jdbcHostname = 'dkbmtbmeuwpgdtb.postgres.database.azure.com'
jdbcPort = '5432'
jdbcDatabase = 'picj5'

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5"


spark.table("bi.validationgeo").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
    .option("dbtable", "public.validationgeo") \
    .option("user", "admintbm@dkbmtbmeuwpgdtb") \
    .option("password", "C_9aVu?qDAia!Rm") \
    .option("ssl", True) \
    .option("sslmode", "require") \
    .option("truncate", "true").mode("overwrite") \
    .save()


#spark.table("bi.validation").write.jdbc(jdbcUrl, "diamonds", connectionProperties)

# COMMAND ----------

#jdbcPassword = dbutils.secrets.get(scope = "kec-kdp-euw-adb-kvt", key = "kec-kdp-cnx-pwd")
jdbcUsername = 'admintbm@dkbmtbmeuwpgdtb'
jdbcHostname = 'dkbmtbmeuwpgdtb.postgres.database.azure.com'
jdbcPort = '5432'
jdbcDatabase = 'picj5'

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5"


spark.table("sig.ligne_evw").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
    .option("dbtable", "public.ligne_evw") \
    .option("user", "admintbm@dkbmtbmeuwpgdtb") \
    .option("password", "C_9aVu?qDAia!Rm") \
    .option("ssl", True) \
    .option("sslmode", "require") \
    .option("truncate", "true").mode("overwrite") \
    .save()


#spark.table("bi.validation").write.jdbc(jdbcUrl, "diamonds", connectionProperties)



# COMMAND ----------



jdbcUsername = 'admintbm@dkbmtbmeuwpgdtb'
jdbcHostname = 'dkbmtbmeuwpgdtb.postgres.database.azure.com'
jdbcPort = '5432'
jdbcDatabase = 'picj5'

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5"


spark.table("sig.arret_evw").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
    .option("dbtable", "public.arret_evw") \
    .option("user", "admintbm@dkbmtbmeuwpgdtb") \
    .option("password", "C_9aVu?qDAia!Rm") \
    .option("ssl", True) \
    .option("sslmode", "require") \
    .option("truncate", "true").mode("overwrite") \
    .save()

spark.table("sig.arret_lieu_evw").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
    .option("dbtable", "public.arret_lieu_evw") \
    .option("user", "admintbm@dkbmtbmeuwpgdtb") \
    .option("password", "C_9aVu?qDAia!Rm") \
    .option("ssl", True) \
    .option("sslmode", "require") \
    .option("truncate", "true").mode("overwrite") \
    .save()

# COMMAND ----------


