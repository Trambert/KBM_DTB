-- Databricks notebook source
-- MAGIC %sql
-- MAGIC --CREATE DATABASE BI;
-- MAGIC 
-- MAGIC DROP TABLE IF EXISTS BI.validation;
-- MAGIC create or replace table BI.validation
-- MAGIC using delta
-- MAGIC location '/mnt/standard/BI/validation'
-- MAGIC as
-- MAGIC SELECT
-- MAGIC  CAST(FACT_VALIDATION.VALIDATIONDATE as date) as date,
-- MAGIC  FACT_VALIDATION.VALIDATIONDATEID AS validationdateid,
-- MAGIC case when ( case when FACT_VALIDATION.PRODUCTID is null and FACT_VALIDATION.VALIDATIONSTATUSID=2 then 1 else 0 end )=1 then 'EXTERNE' else rtrim(PRODUCT.CODE) end as code_produit,
-- MAGIC table_produit.Nom_produits_voyages_comptable as libelle_produit,
-- MAGIC  CASE WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='A' then '59'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='B' then '60'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='C' then '61'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='D' then '62'
-- MAGIC 
-- MAGIC   WHEN VEHICLE.PARKNUM IN (6901,6902,6903) THEN '69'
-- MAGIC   else 
-- MAGIC   CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END
-- MAGIC   end as code_ligne,
-- MAGIC     CASE WHEN FACT_VALIDATION.LINEID=9998 THEN 'DELOCALIZED' ELSE 
-- MAGIC     CASE when (VEHICLE.PARKNUM IN (6901,6902,6903)) then 'Navette Fluviale'
-- MAGIC     else COALESCE(LINE.DESCRIPTION,'LIGNE INCONNU') END END as description_ligne,
-- MAGIC     case when (VEHICLETYPE.DESCRIPTION='TRAMWAY') then 'Tramway' --'Tramway' 
-- MAGIC       WHEN (VEHICLETYPE.DESCRIPTION='URBANBUS' and VEHICLE.PARKNUM NOT IN (6901,6902,6903) ) then 'Bus' --'Bus'
-- MAGIC       WHEN (VEHICLE.PARKNUM IN (6901,6902,6903)) then 'Navette Fluviale' --'navette'
-- MAGIC       else VEHICLETYPE.DESCRIPTION end
-- MAGIC       as code_mode,
-- MAGIC   count(FACT_VALIDATION.VALIDATIONID) as nbre_validation
-- MAGIC FROM
-- MAGIC   billettique.FACT_VALIDATION 
-- MAGIC    LEFT OUTER JOIN billettique.DIM_PRODUCT ON (FACT_VALIDATION.FAREPRODUCTITEMID=DIM_PRODUCT.FAREPRODUCTITEMID)
-- MAGIC    LEFT OUTER JOIN billettique.PRODUCT ON (FACT_VALIDATION.PRODUCTID=PRODUCT.PRODUCTID)
-- MAGIC    LEFT OUTER JOIN billettique.LINE ON (FACT_VALIDATION.LINEID=LINE.LINEID)
-- MAGIC    LEFT OUTER JOIN billettique.DEVICE ON (FACT_VALIDATION.DEVICEID=DEVICE.DEVICEID)
-- MAGIC    LEFT OUTER JOIN billettique.DEVICETYPE ON (DEVICE.DEVICETYPEID=DEVICETYPE.DEVICETYPEID)
-- MAGIC    LEFT OUTER JOIN billettique.VALIDATIONSTATUS ON (FACT_VALIDATION.VALIDATIONSTATUSID=VALIDATIONSTATUS.VALIDATIONSTATUSID)
-- MAGIC    LEFT OUTER JOIN billettique.VEHICLE ON (FACT_VALIDATION.VEHICLEID=VEHICLE.VEHICLEID)
-- MAGIC    LEFT OUTER JOIN billettique.VEHICLETYPE ON (VEHICLETYPE.ID=VEHICLE.VEHICLETYPEID)
-- MAGIC    LEFT OUTER JOIN ref_produit.table_produit ON (table_produit.Code_produit = PRODUCT.CODE)
-- MAGIC 
-- MAGIC WHERE
-- MAGIC DEVICETYPE.DESCRIPTION  IN  ( 'MOBILE TICKETING','VAL'  )
-- MAGIC and
-- MAGIC VALIDATIONSTATUS.CODE='OK'
-- MAGIC 
-- MAGIC --cast(FACT_VALIDATION.VALIDATIONDATE as date)>='2022-02-01'
-- MAGIC GROUP BY
-- MAGIC  CAST(FACT_VALIDATION.VALIDATIONDATE as date),
-- MAGIC  FACT_VALIDATION.VALIDATIONDATEID,
-- MAGIC  case when ( case when FACT_VALIDATION.PRODUCTID is null and FACT_VALIDATION.VALIDATIONSTATUSID=2 then 1 else 0 end )=1 then 'EXTERNE' else rtrim(PRODUCT.CODE) end ,
-- MAGIC  table_produit.Nom_produits_voyages_comptable,
-- MAGIC  CASE WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='A' then '59'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='B' then '60'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='C' then '61'
-- MAGIC   WHEN (CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END)='D' then '62'
-- MAGIC 
-- MAGIC   WHEN VEHICLE.PARKNUM IN (6901,6902,6903) THEN '69'
-- MAGIC   else 
-- MAGIC   CASE WHEN FACT_VALIDATION.LINEID=9998 THEN '9998' ELSE LINE.CODE END
-- MAGIC   end,
-- MAGIC     CASE WHEN FACT_VALIDATION.LINEID=9998 THEN 'DELOCALIZED' ELSE 
-- MAGIC     CASE when (VEHICLE.PARKNUM IN (6901,6902,6903)) then 'Navette Fluviale'
-- MAGIC     else COALESCE(LINE.DESCRIPTION,'LIGNE INCONNU') END END,
-- MAGIC     case when (VEHICLETYPE.DESCRIPTION='TRAMWAY') then 'Tramway' --'Tramway' 
-- MAGIC       WHEN (VEHICLETYPE.DESCRIPTION='URBANBUS' and VEHICLE.PARKNUM NOT IN (6901,6902,6903) ) then 'Bus' --'Bus'
-- MAGIC       WHEN (VEHICLE.PARKNUM IN (6901,6902,6903)) then 'Navette Fluviale' --'navette'
-- MAGIC       else VEHICLETYPE.DESCRIPTION end

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #jdbcPassword = dbutils.secrets.get(scope = "kec-kdp-euw-adb-kvt", key = "kec-kdp-cnx-pwd")
-- MAGIC jdbcUsername = 'admintbm@dkbmtbmeuwpgdtb'
-- MAGIC jdbcHostname = 'dkbmtbmeuwpgdtb.postgres.database.azure.com'
-- MAGIC jdbcPort = '5432'
-- MAGIC jdbcDatabase = 'picj5'
-- MAGIC 
-- MAGIC driver = "org.postgresql.Driver"
-- MAGIC url = "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5"
-- MAGIC 
-- MAGIC 
-- MAGIC spark.table("bi.validation").write \
-- MAGIC     .format("jdbc") \
-- MAGIC     .option("url", "jdbc:postgresql://dkbmtbmeuwpgdtb.postgres.database.azure.com:5432/picj5") \
-- MAGIC     .option("dbtable", "public.validation") \
-- MAGIC     .option("user", "admintbm@dkbmtbmeuwpgdtb") \
-- MAGIC     .option("password", "C_9aVu?qDAia!Rm") \
-- MAGIC     .option("ssl", True) \
-- MAGIC     .option("sslmode", "require") \
-- MAGIC     .option("truncate", "true").mode("overwrite") \
-- MAGIC     .save()
-- MAGIC 
-- MAGIC 
-- MAGIC #spark.table("bi.validation").write.jdbc(jdbcUrl, "diamonds", connectionProperties)