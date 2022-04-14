# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return True
    else:
        return False

    
def createOrUpdateDelta(newDf, save_path, key, object):
    #Check if Delta table exists
    isDelta = DeltaTable.isDeltaTable(spark, save_path)
    if isDelta:
        deltaTable = DeltaTable.forPath(spark, save_path)
        deltaTable.alias("old").merge(
            source = newDf.alias("updates"),
            condition = expr("old."+key+" = updates."+key)
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        table_name = object
        newData.write.format('delta').mode('overwrite').save(save_path)
        display(spark.sql("DROP TABLE IF EXISTS titan_recette." + table_name))
        display(spark.sql("CREATE TABLE titan_recette." + table_name + " USING DELTA LOCATION '" + save_path + "'"))

def createOrUpdateDeltaWithPartition(newDf, save_path, key, object, partitionBy):
    #Check if Delta table exists
    isDelta = DeltaTable.isDeltaTable(spark, save_path)
    if isDelta:
        deltaTable = DeltaTable.forPath(spark, save_path)
        deltaTable.alias("old").merge(
            source = newDf.alias("updates"),
            condition = expr("old."+key+" = updates."+key)
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        table_name = object
        newData.write.partitionBy(partitionBy).format('delta').mode('overwrite').save(save_path)
        display(spark.sql("DROP TABLE IF EXISTS titan_recette." + table_name))
        display(spark.sql("CREATE TABLE titan_recette." + table_name + " USING DELTA LOCATION '" + save_path + "'"))
    
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

# TITAN_RECETTE / VENTES / 2022 / 03 / 17
Source = "TITAN_RECETTE"
Object = "VENTES"
TableKey = "VECLEUNIK"
PartitionBy = ""

#year,month,day=LastLoadDate.split('-')
    

    
save_path = '/mnt/curated/'+Source+'/'+Object
#Read new data
newData = spark.read.parquet(f"mnt/raw/{Source}/{Object}/2022/03/17/{Object}*.parquet")
if(len(PartitionBy)>0):
    createOrUpdateDeltaWithPartition(newData, save_path, TableKey, Object, PartitionBy)
else:
    createOrUpdateDelta(newData, save_path, TableKey, Object)



# COMMAND ----------

# MAGIC %sql
# MAGIC --Validite_debut
# MAGIC --Validite_fin
# MAGIC --ORI_DATE
# MAGIC --DEST_DATE
# MAGIC /*
# MAGIC select distinct(Date_vente)
# MAGIC from titan_recette.ventes as v
# MAGIC --where length(DEST_DATE)>0
# MAGIC order by Date_vente asc
# MAGIC 
# MAGIC select distinct(DEST_DATE)
# MAGIC from titan_recette.ventes
# MAGIC --where Validite_debut = '02080731'
# MAGIC order by DEST_DATE desc
# MAGIC */
# MAGIC select 
# MAGIC p.CODFAMPRO as CodeFamilleProduit,
# MAGIC pp.Libelle as LibFamilleProduit,
# MAGIC p.LIBPRO as Produit,
# MAGIC year(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')) as year,
# MAGIC month(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')) as month,
# MAGIC sum(v.Mt_vente) as Mt_Vente,
# MAGIC sum(Quantite_vendue) as Quantite_vendue
# MAGIC from titan_recette.ventes v
# MAGIC inner join titan_recette.produit p on p.CODPRO = v.Code_produit
# MAGIC inner join ref_produit.table_produit pp on pp.Code_produit = v.Code_produit
# MAGIC where year(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')) >= 2020
# MAGIC group by 
# MAGIC year(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')),
# MAGIC month(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')),
# MAGIC p.LIBPRO,
# MAGIC p.CODFAMPRO,
# MAGIC pp.Libelle
# MAGIC order by 
# MAGIC year(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS')),
# MAGIC month(to_date(Date_transaction,'yyyy-MM-dd HH:mm:SS'))

# COMMAND ----------


