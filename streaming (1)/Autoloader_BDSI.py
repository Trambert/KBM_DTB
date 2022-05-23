# Databricks notebook source
#Initialize File Schema for dataloader
from pyspark.sql import Row
import json


Source='BDSI'
ListObjects = ['ARRETCHN_ACH','ARRET_ARR','ARRET_FLEXO_AFL','ARRET_PF_APF','CHAINAGE_CHN','CIBLE_CIB','CORRESIV_COR','COURSE_CRS','DEPOT_DEP','DESTINATION_DST','DEVIATION_PF_DPF','DEVIATION_PROG_DPR','EXPLOITANT_EXP','HORAIRE_HOR','JOUR_JOU','LIEUX_PUBLICS_LPU','LIGNE_LIG','MEDIA_MED','MESSAGE_MSG','PARAM_PAR','PROGRAMME_PRG','SURVEILLANCE_SUR','VEHICULE_VEH','ZONE_FLEXO_ZFL']

for i in ListObjects:
    Object = i
    newData = spark.read.parquet("mnt/raw/"+Source+"/"+Object+"/*/*/*/"+Object+"*.parquet")
    schema = newData.schema.json()

    dbutils.fs.rm(f"/mnt/raw/DeltaSchema/{Object}.json", True)
    with open(f"/dbfs/mnt/raw/DeltaSchema/{Object}.json", "w") as f:
        json.dump(schema, f)
      #f.write(newData.schema.json())
        



# COMMAND ----------

#Start Autoloader functions looping over Objects name to stream (from BDSI)
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name
import json

def importFromStream(Object):
    with open(f"/dbfs/mnt/raw/DeltaSchema/{Object}.json", 'r') as F:  
        saved_schema = json.load(F)

    schema = StructType.fromJson(json.loads(saved_schema))
    readStream = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'Parquet') \
    .schema(schema) \
    .load(f"/mnt/raw/BDSI/{Object}/*/*/*/{Object}*.parquet") \
    .select("*","_metadata.file_path","_metadata.file_name","_metadata.file_size","_metadata.file_modification_time" )

    #print(ARRET_ARR)

    readStream.writeStream.format('delta') \
     .option('checkpointLocation', f"/mnt/raw/checkpoint/{Object}") \
     .start(f"/mnt/raw/delta/{Object}")

ListObjects = ['ARRETCHN_ACH','ARRET_ARR','ARRET_FLEXO_AFL','ARRET_PF_APF','CHAINAGE_CHN','CIBLE_CIB','CORRESIV_COR','COURSE_CRS','DEPOT_DEP','DESTINATION_DST','DEVIATION_PF_DPF','DEVIATION_PROG_DPR','EXPLOITANT_EXP','HORAIRE_HOR','JOUR_JOU','LIEUX_PUBLICS_LPU','LIGNE_LIG','MEDIA_MED','MESSAGE_MSG','PARAM_PAR','PROGRAMME_PRG','SURVEILLANCE_SUR','VEHICULE_VEH','ZONE_FLEXO_ZFL']

for i in ListObjects:
    Object = i
    importFromStream(Object)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name
from delta.tables import *
import json

with open(f"/dbfs/mnt/raw/DeltaSchema/ARRET_ARR.json", 'r') as F:  
    saved_schema = json.load(F)

schema = StructType.fromJson(json.loads(saved_schema))
ARRET_ARR = spark.readStream.format('cloudFiles') \
.option('cloudFiles.format', 'Parquet') \
.schema(schema) \
.load("/mnt/raw/BDSI/ARRET_ARR/*/*/*/*.parquet") \
.select("*","_metadata" )

#print(ARRET_ARR)

ARRET_ARR.writeStream.format('delta') \
 .option('checkpointLocation', "/mnt/raw/checkpoint/ARRET_ARR") \
 .start("/mnt/raw/delta/ARRET_ARR")

# COMMAND ----------


