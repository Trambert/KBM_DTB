# Databricks notebook source
# filiale_name = "besancon"
# type_heure = 'string'
# zone_vac = "zoneA"

filiale_name = "autocarPlanche"
type_heure = 'mktime'
zone_vac = "zoneA"

# filiale_name = "transpole"
# type_heure = 'float'
# zone_vac = "zoneB"


# filiale_name = "aerolis"
# type_heure = 'mktime'
# zone_vac = "zoneA"

# COMMAND ----------

# DBTITLE 1,Initialize import, adls mount point, function and files path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SQLContext
from pyspark.sql import GroupedData
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkFiles
from slugify import slugify
from pyspark.sql import DataFrame
from pyspark.sql.functions import date_format
from pyspark.sql.functions import substring
import sys

import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import pandas as pd
import subprocess

import googlemaps


#Mount ADLS to read and write files
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "7c09538a-5fe1-4b9f-8989-4481d80bb578", #ID du service principal de dev
           "dfs.adls.oauth2.credential": "=Bn+kX20b7Dyhsd5yEXXxLq/CtQNQJD/", #Secret du service principal de dev
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/7124e463-2734-41bf-bddb-3e475374f94c/oauth2/token", #endpoint du Tenant
           "dfs.adls.oauth2.access.token.provider": "org.apache.hadoop.fs.adls.oauth2.ConfCredentialBasedAccessTokenProvider"}

mountPoint = "/mnt/ADLSLab" #chemin sous lequel l'ADLS sera accessible dans Databricks

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = "adl://ksasdteuwdls.azuredatalakestore.net/developpement/dev_datalab", #chemin réel du répertoire DataLab dans l'ADLS
    mount_point = mountPoint,
    extra_configs = configs)
else:
  print("Une ressource est déjà présente à ce point de montage")


#Files path
result_path = "/mnt/ADLSLab/projets/piloteAbsenteisme/" + filiale_name + "/results"
input_path = "/mnt/ADLSLab/projets/piloteAbsenteisme/talend_objects/" + filiale_name 
cache_path = "/mnt/ADLSLab/projets/piloteAbsenteisme/cache"

def harmonize_schemas_and_combine(df_left, df_right, default_string="donnee manquante", default_number=0):
    left_types = {f.name: f.dataType for f in df_left.schema}
    right_types = {f.name: f.dataType for f in df_right.schema}
    left_fields = set((f.name, f.dataType, f.nullable) for f in df_left.schema)
    right_fields = set((f.name, f.dataType, f.nullable) for f in df_right.schema)

    # First go over left-unique fields
    for l_name, l_type, l_nullable in left_fields.difference(right_fields):
        if l_name in right_types:
            r_type = right_types[l_name]
            if l_type != r_type:
                raise TypeError("Union failed. Type conflict on field %s. left type %s, right type %s" % (l_name, l_type, r_type))
            else:
                raise TypeError("Union failed. Nullability conflict on field %s. left nullable %s, right nullable %s"  % (l_name, l_nullable, not(l_nullable)))
        print(l_type)
        df_right = df_right.withColumn(l_name, F.lit(None).cast(l_type))

    # Now go over right-unique fields
    for r_name, r_type, r_nullable in right_fields.difference(left_fields):
        if r_name in left_types:
            l_type = left_types[r_name]
            if r_type != l_type:
                raise TypeError("Union failed. Type conflict on field %s. right type %s, left type %s" % (r_name, r_type, l_type))
            else:
                raise TypeError("Union failed. Nullability conflict on field %s. right nullable %s, left nullable %s" % (r_name, r_nullable, not(r_nullable)))
        df_left = df_left.withColumn(r_name, F.lit(None).cast(r_type))    

    # Make sure columns are in the same order
    df_left = df_left.select(df_right.columns)

    return df_left.union(df_right)

#LECTURE CSV
def read_csv(filename, schema=None, encoding=None):
  inp = os.path.join(input_path, filename)
  if encoding:
    return spark.read.option("encoding", encoding).option("delimiter", ";").csv(inp, header=True)

  if schema:
    return spark.read.schema(schema).option("delimiter", ";").csv(inp, header=True)
  else:
    return spark.read.option("delimiter", ";").csv(inp, header=True)

  
#ECRITURE CSV
def export_result_df(sparkdf, path, input=False, cache=False, encoding=None):
  if cache:
    dirpath = cache_path
  else:
    dirpath = input_path if input else result_path
  temppath = os.path.join(dirpath, path.replace(".csv", ".tmp"))
  outputpath = os.path.join(dirpath, path)
  
  if encoding:
    sparkdf.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("encoding", encoding).option("decimal", ",").save(temppath)
  else:
    sparkdf.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("decimal", ",").save(temppath)

  partition_path = dbutils.fs.ls(temppath)
  csv_files = [f.name for f in partition_path if f.name.endswith(".csv")]
  non_csv_files = [f.name for f in partition_path if not f.name.endswith(".csv")]
  for f in non_csv_files:
    ff = os.path.join(temppath, f)
    dbutils.fs.rm(ff, recurse=True)
  copyMerge(temppath, outputpath, columns=list(sparkdf.schema.names), debug=False, overwrite=True, deleteSource=True)
  print('Result exported to ' + outputpath)

  
#MERGE ALL PART OF A CREATED FILE, LAUCH AUTOMATICALLY AFTER A FILE CREATION
def copyMerge (src_dir, dst_file, columns=None, overwrite=False, deleteSource=False, debug=False):
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)

    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    files.sort(key=lambda f: str(f))

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)
    if columns:
      cols = ";".join(columns) + "\n"
      filepath = os.path.join("/dbfs"+src_dir, "head.csv")
      file = open(filepath,"w") 
      file.write(cols)
      file.close()
      show_file(os.path.join(src_dir, "head.csv"))
      in_stream = fs.open(hadoop.fs.Path(os.path.join(src_dir, "head.csv")))
      hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for file in files:
            if debug: 
                print("Appending file {} into {}".format(file, dst_file))

            in_stream = fs.open(file)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        fs.delete(hadoop.fs.Path(src_dir), True)    # True=recursive
        if debug:
            print("Source directory {} removed.".format(src_dir))
            
def show_file(path):
  print("Display " + path + ":")
  file = open("/dbfs"+path, "r") 
  print(file.read())
  file.close()
  

def cache(df, file, ignore_save=False):
  fn = filiale_name + "_" +file
  if not ignore_save:
    export_result_df(df, fn, cache=True)   
  inp = os.path.join(cache_path, fn)
  ret = spark.read.option("delimiter", ";").option("inferSchema", "true").csv(inp, header=True)
  date_list = ["date_roulement", "DATE_DEBUT", "DATE_FIN"]
  for c in date_list:
    if c not in ret.schema.names:
      continue
    ret = ret.withColumn(c, F.col(c).cast("date"))\

  return ret
  
def fill_nulls_2sides(df, target, id="id_salarie", date="date_roulement"):
  df = fill_nulls(df, target, id, date, True)
  df = fill_nulls(df, target, id, date, False)  
  return df

def bcache(df):
  df = df.cache()
  df.count()
  return df


def fill_nulls(df, target, id, date, ffill=True):
  datatype = df.schema[target].dataType
  na_val = -1
  if isinstance(datatype, StringType):
    na_val = "-1"
    
  if ffill:
    side = date
  else:
    side = F.desc(date)
  
  df_na = df.withColumn("target_bis", F.when(F.col(target).isNull(), na_val).otherwise(F.col(target)))

  df_na = df_na.withColumn("target_null", F.when(F.col(target).isNull(), -1).otherwise(0))#.fillna(-1, "target_bis")
  lag_win = Window.partitionBy(id).orderBy(side)
  lag = df_na.withColumn('target_lag', F.lag("target_bis", default=-1).over(lag_win))
  
  switch = lag.withColumn('target_change',
                          ((lag["target_bis"] != lag['target_lag']) & (lag["target_null"] != -1)).cast('integer'))

  switch_win = Window.partitionBy(id).orderBy(side).rowsBetween(-sys.maxsize, 0)
  switch_sess = switch.withColumn('sub_session', F.sum("target_change").over(switch_win))

  fid_win = Window.partitionBy(id, 'sub_session').orderBy(side)
  fid = switch_sess.withColumn('nn_target', F.first(target).over(fid_win))

  ff = fid.drop('target_lag', "target_bis", 'target_change', "sub_session", "target_null")\
          .drop(target).withColumnRenamed('nn_target',target)
  return ff



def get_cols_starting_with(df, prefix=None):
  return [c for c in list(df.schema.names) if c.startswith(prefix)]

def get_col_unique(df, col):
  values = [getattr(i, col) for i in df.select(col).distinct().collect()]
  values = [v for v in values if v is not None]
  return values

def binarize(df, col, prefix=None, column_values=None, slug=True):
  # Binarization des classes
  if column_values:
    values = column_values
  else:
    values = get_col_unique(df, col)
  values = [v for v in values if v != "manquant"]

  for v in values:
    name = slugify(v) if slug else v
    if prefix:
      name = prefix + "_" + name
    df = df.withColumn(name, F.when(F.col(col)==v,1).otherwise(0))
  return df


def get_matrice_od(df_orig, reload=False, filename="matrice_od.csv", orig="adresse_salarie", dest="adresse_depot"):
  if reload:
    df = df_orig.toPandas()
    gmaps = googlemaps.Client(key="AIzaSyBRjyWasjR6iiRS5CYhXxjr_paWeYVelnU")

    out_dict = []
    dd = df.to_dict("records")
    for d in dd:
      origin = d[orig]
      destination = d[dest]

      if origin is None or destination is None:
        print("origin: ", origin, "dest: ", destination)
        continue
      try:
        ret = gmaps.distance_matrix(origin, destination, mode='driving')#["rows"][0]["elements"][0]["distance"]["value"]
        print(ret)
        dcopy = dict(d)
        dcopy["duration_s"] = ret["rows"][0]["elements"][0]["duration"]["value"]
        dcopy["distance_m"] = ret["rows"][0]["elements"][0]["distance"]["value"]
        out_dict.append(dcopy)
        print(dcopy)

      except:
        print(d)
#       break
    df_od = spark.createDataFrame(out_dict)
    export_result_df(df_od, filename, input=True)  
    
  inp = os.path.join(input_path, filename)
  df_od = spark.read.option("delimiter", ";").option("inferSchema", "true").csv(inp, header=True)
  return df_od

# COMMAND ----------

# DBTITLE 1,Read objects files, define all fields - filiale, centre, depot et ligne
#Structure et import des objets filiale, centre et depot########################################
filialeSchema = StructType([
        StructField("id_filiale", IntegerType(), True),
        StructField("lib_filiale", StringType(), True),
        StructField("adresse_filiale", StringType(), True),
        StructField("cp_filiale", StringType(), True),
        StructField("lat_filiale", DoubleType(), True),
        StructField("lon_filiale", DoubleType(), True)
])

centreSchema = StructType([
        StructField("id_filiale", StringType(), True),
        StructField("id_centre", StringType(), True),
        StructField("lib_centre", IntegerType(), True),
        StructField("adresse_centre", StringType(), True),
        StructField("cp_centre", StringType(), True),
        StructField("lat_centre", DoubleType(), True),
        StructField("lon_centre", DoubleType(), True)
])

depotSchema = StructType([
        StructField("id_filiale", StringType(), True),      
        StructField("id_centre", StringType(), True),
        StructField("id_depot", IntegerType(), True),
        StructField("lib_depot", StringType(), True),
        StructField("adresse_depot", StringType(), True),
        StructField("cp_depot", StringType(), True),
        StructField("lat_depot", DoubleType(), True),
        StructField("lon_depot", DoubleType(), True)
])


ligneSchema = StructType([
        StructField("code_unite", StringType(), True),      
        StructField("id_ligne_service", StringType(), True),
        StructField("lib_ligne", StringType(), True)
])

#filiale = read_csv("filiale.csv")
if filiale_name == "aerolis":
  CEN = read_csv("centre.csv").alias("CEN")
  LIG = spark.createDataFrame(sc.emptyRDD(), ligneSchema).alias('LIG')
  DEP = spark.createDataFrame(sc.emptyRDD(), depotSchema).alias('DEP')
  
else:
  DEP = read_csv("depot.csv", encoding="UTF-8").alias('DEP')
  LIG = read_csv("ligne.csv").alias('LIG')

  if filiale_name == "transpole":
    LIG = LIG.withColumnRenamed('typo', 'LIGNE_TYPO')
    LIG = LIG.withColumn("est_ligne", F.col('est_ligne').cast("boolean"))
    LIG = LIG.withColumn("voiture_releve", F.col('voiture_releve').cast("boolean"))
  else:
    LIG.withColumn("est_ligne", F.lit(True))

  ligne_lib_list = get_col_unique(LIG, "lib_ligne") + ["missing"]
# ligne_typo_list = get_col_unique(LIG, "LIGNE_TYPO")
#################################################################################################


# COMMAND ----------

# DBTITLE 1,Read objects files, define all fields - salarie, contrat, fonction, remuneration
#Structure et import des objets salarie, contrat, fonction, remuneration ########################
salarieSchema = StructType([
        StructField("id_salarie", StringType(), True),
        StructField("sexe", StringType(), True),
        StructField("sexe_lib", StringType(), True),
        StructField("civilite", StringType(), True),
        StructField("civilite_lib", StringType(), True),
        StructField("situation_familiale", StringType(), True),
        StructField("nombre_enfants", IntegerType(), True),
        StructField("cp_salarie", StringType(), True),
        StructField("ville_salarie", StringType(), True),
        StructField("pays_salarie", StringType(), True),
        StructField("nb_permis", IntegerType(), True),
        StructField("date_naissance", DateType(), True),
        StructField("date_naissance_enfant_1", DateType(), True),
        StructField("date_naissance_enfant_2", DateType(), True),
        StructField("date_naissance_enfant_3", DateType(), True),
        StructField("date_naissance_enfant_4", DateType(), True),
        StructField("date_naissance_enfant_5", DateType(), True),
        StructField("date_naissance_enfant_6", DateType(), True),
        StructField("date_naissance_enfant_7", DateType(), True),
        StructField("date_naissance_enfant_8", DateType(), True),
        StructField("date_naissance_enfant_9", DateType(), True),
        StructField("date_naissance_enfant_10", DateType(), True)
])

contratSchema = StructType([
        StructField("id_salarie", StringType(), True),
        StructField("mois_annee", DateType(), True),
        StructField("date_debut_contrat", DateType(), True),
        StructField("date_fin_contrat", DateType(), True),
        StructField("date_fin_periode_essai", DateType(), True),
        StructField("nature_contrat", StringType(), True),
        StructField("date_embauche_societe", DateType(), True),
        StructField("date_depart_societe", DateType(), True),
        StructField("motif_depart_societe", StringType(), True),
        StructField("date_anciennete_profession", DateType(), True),
        StructField("convention_collective", StringType(), True),
        StructField("lieu_travail_different", BooleanType(), True),
        StructField("adresse_lieu_travail", StringType(), True),
        StructField("code_postal_lieu_travail", StringType(), True),
        StructField("commune_lieu_travail", StringType(), True),
        StructField("id_categorie_professionnelle", StringType(), True),
        StructField("lib_categorie_professionnelle", StringType(), True),
        StructField("temps_travail_pourcent", DoubleType(), True),
        StructField("temps_travail_mensuel", DoubleType(), True),
        StructField("temps_mal_mensuel", DoubleType(), True),
        StructField("temps_mat_mensuel", DoubleType(), True),
        StructField("temps_mtt_mensuel", DoubleType(), True),
        StructField("temps_at_mensuel", DoubleType(), True),
        StructField("temps_attraj", DoubleType(), True)
])

fonctionSchema = StructType([
        StructField("id_salarie", StringType(), True),
        StructField("mois_annee", DateType(), True),
        StructField("departement_salarie", StringType(), True),
        StructField("service_salarie", StringType(), True),
        StructField("unite_salarie", StringType(), True),
        StructField("emploi_occupe", StringType(), True),
        StructField("categorie_salarie", StringType(), True),
        StructField("date_entree_poste", DateType(), True),
        StructField("date_sortie_poste", DateType(), True),
        StructField("id_activite", StringType(), True),
        StructField("lib_activite", StringType(), True)
])

remunerationSchema = StructType([
        StructField("id_salarie", StringType(), True),
        StructField("mois_annee", DateType(), True),
        StructField("Coefficient", DoubleType(), True),
        StructField("BRUT_MENSUEL", DoubleType(), True),
        StructField("_400_B_Hr_Contrat", DoubleType(), True),
        StructField("_544_B_Hr_Trav", DoubleType(), True),
        StructField("_540_B_Hr_Payees", DoubleType(), True),
        StructField("_302_Base_Anc", DoubleType(), True),
        StructField("_303_Anc", DoubleType(), True),
        StructField("_305_Tx_Anc", DoubleType(), True),
        StructField("_304_Sal_Base", DoubleType(), True),
        StructField("_315_Rap_Sal", DoubleType(), True),
        StructField("_375_Sal_Prof", DoubleType(), True),
        StructField("_376_Anc_Prof", DoubleType(), True),
        StructField("_370_Sal_Apprenti", DoubleType(), True),
        StructField("_371_Ancien_Apprenti", DoubleType(), True),
        StructField("_306_Pr_Fonct1", DoubleType(), True),
        StructField("_304_tx_hor", DoubleType(), True),
        StructField("_307_Diff_reclassement1", DoubleType(), True),
        StructField("_500_Rap_Sal_NAO", DoubleType(), True),
        StructField("_502_Rap_Anc_NAO", DoubleType(), True),
        StructField("_120_13eMois", DoubleType(), True),
        StructField("_125_13M_Depart", DoubleType(), True),
        StructField("_910_IJ_MAL", DoubleType(), True),
        StructField("_940_IJ_ATTJ", DoubleType(), True),
        StructField("_930_IJ_AT", DoubleType(), True),
        StructField("_945_IJ_MTT", DoubleType(), True),
        StructField("_610_Abs_MAL", DoubleType(), True),
        StructField("_616_MAL_100_", DoubleType(), True),
        StructField("_630_Abs_MAT", DoubleType(), True),
        StructField("_635_Ind_MAT", DoubleType(), True),
        StructField("_640_Abs_PAT", DoubleType(), True),
        StructField("_645_Abs_AT", DoubleType(), True),
        StructField("_650_Ind_AT", DoubleType(), True),
        StructField("_665_Abs_ATJ", DoubleType(), True),
        StructField("_670_Ind_ATJ", DoubleType(), True),
        StructField("_730_Abs_MTT", DoubleType(), True),
        StructField("_735_Ind_MTT", DoubleType(), True),
        StructField("_210_CSS", DoubleType(), True),
        StructField("_210_CSS_BASE", DoubleType(), True),
        StructField("_220_conge_parental", DoubleType(), True),
        StructField("_220_conge_par_mont", DoubleType(), True),
        StructField("_214_Abs_DIV", DoubleType(), True),
        StructField("_215_Abs_Auto_NP", DoubleType(), True),
        StructField("_218_Abs_Non_Auto_NP", DoubleType(), True),
        StructField("_864_PAIEM_NUIT", DoubleType(), True),
        StructField("_864_PAIEM_NUIT1", DoubleType(), True),
        StructField("_845_HC110", DoubleType(), True),
        StructField("_845_HC_110", DoubleType(), True),
        StructField("_855_Nbr_H_Exced", DoubleType(), True),
        StructField("_865_Nbr_H_Dim", DoubleType(), True),
        StructField("_866_Nbr_H_FL", DoubleType(), True),
        StructField("_874_Nbr_H_Nuit_25_", DoubleType(), True),
        StructField("_872_Nbr_H_Nuit_50_", DoubleType(), True),
        StructField("_960_Major_HN40_", DoubleType(), True),
        StructField("_855_Hr_Exced", DoubleType(), True),
        StructField("_865_Hr_Dim", DoubleType(), True),
        StructField("_866_Hr_FL", DoubleType(), True),
        StructField("_874_Hr_Nuit_25_", DoubleType(), True),
        StructField("_872_Hr_Nuit_50_", DoubleType(), True),
        StructField("_960_Major_HN40_1", DoubleType(), True),
        StructField("_525_Ind_CP", DoubleType(), True),
        StructField("_526_Abs_CP", DoubleType(), True),
        StructField("_527_Ind_AgeAnc", DoubleType(), True),
        StructField("_529_Abs_AgeAnc", DoubleType(), True),
        StructField("_53I_Regul_10e_CP", DoubleType(), True),
        StructField("_660_BONUS_PNA", DoubleType(), True),
        StructField("_661PRIME_TRAM", DoubleType(), True),
        StructField("_199_P_CONTROLE", DoubleType(), True),
        StructField("_290_PV", DoubleType(), True),
        StructField("_161_PRIME_ASTR", DoubleType(), True),
        StructField("_160_PMP", DoubleType(), True),
        StructField("_195_Pr_Except", DoubleType(), True),
        StructField("_198_PO", DoubleType(), True),
        StructField("_200_Pr_Outil", DoubleType(), True),
        StructField("_314_Nb_Panier_S", DoubleType(), True),
        StructField("_314_Panier_Soumis", DoubleType(), True),
        StructField("_831_Nb_H_VM", DoubleType(), True),
        StructField("_831_Hr_VM", DoubleType(), True),
        StructField("_833_Nb_H_VOTE", DoubleType(), True),
        StructField("_833_H_VOTE", DoubleType(), True),
        StructField("_245_Medailles_S", DoubleType(), True)
])

salarie = read_csv("salarie.csv", encoding="cp1252")
contrat = read_csv("contrat.csv")
fonction = read_csv("fonction.csv")
remuneration = read_csv("remuneration.csv")

if filiale_name == "transpole":
  primes = read_csv("prime.csv")
  retards = read_csv("retards.csv")
  PRI = primes.alias('PRI')
  RET = retards.alias('RET')
else:
  PRI = spark.createDataFrame([ ("x","1970-01-01","", 0) ],
                              ["id_salarie", "date_roulement", "lib_prime", "montant_prime"]).alias('PRI')
  RET = spark.createDataFrame([ ("x","1970-01-01", 0) ],
                              ["id_salarie", "date_roulement", "retard"]).alias('RET')
  
PRI = PRI.withColumn('date_roulement', F.to_date(F.col("date_roulement"),"yyyy-MM-dd"))
PRI = PRI.withColumn('montant_prime', F.regexp_replace('montant_prime', ',', '.').cast("double"))
RET = RET.withColumn('date_roulement', F.to_date(F.col("date_roulement"),"yyyy-MM-dd"))
#################################################################################################

#Création des alias des fichiers ################################################################
CON = contrat.alias('CON')
FON = fonction.alias('FON')
REM = remuneration.alias('REM')
SAL = salarie.alias('SAL')

#################################################################################################
# display(CON.groupBy("mois_annee").count())

dformat = "yyyy-MM-dd"

#Cast des types de champs présents dans les imports #############################################
CON = CON.withColumn('date_debut_contrat', F.to_date(F.col("date_debut_contrat"),dformat))\
          .withColumn('date_fin_contrat', F.to_date(F.col("date_fin_contrat"),dformat))\
          .withColumn('date_fin_periode_essai', F.to_date(F.col("date_fin_periode_essai"),dformat))\
          .withColumn('date_embauche_societe', F.to_date(F.col("date_embauche_societe"),dformat))\
          .withColumn('date_depart_societe', F.to_date(F.col("date_depart_societe"),dformat))\
          .withColumn('date_anciennete_profession', F.to_date(F.col("date_anciennete_profession"),dformat))\
          .withColumn('temps_travail_pourcent', F.regexp_replace('temps_travail_pourcent', ',', '.').cast('double'))\
          .withColumn('temps_travail_mensuel', F.regexp_replace('temps_travail_mensuel', ',', '.').cast('double'))\
          .withColumn('temps_mal_mensuel', F.regexp_replace('temps_mal_mensuel', ',', '.').cast('double'))\
          .withColumn('temps_mat_mensuel', F.regexp_replace('temps_mat_mensuel', ',', '.').cast('double'))\
          .withColumn('temps_mtt_mensuel', F.regexp_replace('temps_mtt_mensuel', ',', '.').cast('double'))\
          .withColumn('temps_at_mensuel', F.regexp_replace('temps_at_mensuel', ',', '.').cast('double'))\
          .withColumn('temps_attraj', F.regexp_replace('temps_attraj', ',', '.').cast('double'))

SAL = SAL.withColumn('date_naissance', F.to_date(F.col('date_naissance'),'yyyy'))\
          .withColumn('date_naissance_enfant_1', F.to_date(F.col('date_naissance_enfant_1'),'yyyy'))\
          .withColumn('date_naissance_enfant_2', F.to_date(F.col('date_naissance_enfant_2'),'yyyy'))\
          .withColumn('date_naissance_enfant_3', F.to_date(F.col('date_naissance_enfant_3'),'yyyy'))\
          .withColumn('date_naissance_enfant_4', F.to_date(F.col('date_naissance_enfant_4'),'yyyy'))\
          .withColumn('date_naissance_enfant_5', F.to_date(F.col('date_naissance_enfant_5'),'yyyy'))\
          .withColumn('date_naissance_enfant_6', F.to_date(F.col('date_naissance_enfant_6'),'yyyy'))\
          .withColumn('date_naissance_enfant_7', F.to_date(F.col('date_naissance_enfant_7'),'yyyy'))\
          .withColumn('date_naissance_enfant_8', F.to_date(F.col('date_naissance_enfant_8'),'yyyy'))\
          .withColumn('date_naissance_enfant_9', F.to_date(F.col('date_naissance_enfant_9'),'yyyy'))\
          .withColumn('date_naissance_enfant_10', F.to_date(F.col('date_naissance_enfant_10'),'yyyy'))\
          .withColumn('nombre_enfants', F.regexp_replace('nombre_enfants', ',', '.').cast('integer'))
if "code_postal" in SAL.schema.names:
    SAL = SAL.withColumnRenamed('code_postal', "cp_salarie")

FON = FON.withColumn('date_entree_poste', F.to_date(F.col("date_entree_poste"),"yyyy-MM-dd"))\
          .withColumn('date_sortie_poste', F.to_date(F.col("date_sortie_poste"),"yyyy-MM-dd"))

# REM = REM.withColumn('BRUT_MENSUEL', F.regexp_replace('BRUT_MENSUEL', ',', '.').cast('double'))
          #.withColumn('Coefficient', F.col("Coefficient").cast('integer'))

if "BRUT_MENSUEL" in REM.schema.names:
    REM = REM.withColumn('BRUT_MENSUEL', F.regexp_replace('BRUT_MENSUEL', ',', '.').cast("double"))
else:
    REM = REM.withColumn('BRUT_MENSUEL', F.regexp_replace('SALAIRE__DE_BASE', ',', '.').cast("double"))
  
if "_400_B_Hr_Contrat" in REM.schema.names:
   REM = REM.withColumn('_400_B_Hr_Contrat', F.regexp_replace('_400_B_Hr_Contrat', ',', '.').cast("double"))\
                   .withColumn('_304_tx_hor', F.regexp_replace('_304_tx_hor', ',', '.').cast("double"))\

REM = REM.filter(REM.mois_annee.isNotNull() & REM.BRUT_MENSUEL.isNotNull()  & (REM.BRUT_MENSUEL!=0))
# display(REM)

#################################################################################################

# COMMAND ----------

# DBTITLE 1,Read objects files, define all fields - service
#Structure et import ds objets services et accidentologie #########################################################
serviceSchema = StructType([
        StructField("id_salarie", StringType(), True),
        StructField("type_planification", StringType(), True),
        StructField("date_roulement", DateType(), True),
        StructField("type_roulement_id", StringType(), True),
        StructField("type_roulement_lib", StringType(), True),
        StructField("position_roulement", StringType(), True),
        StructField("heure_debut_service", StringType(), True),
        StructField("heure_fin_service", StringType(), True),
        StructField("lib_lieu_debut_service", StringType(), True),
        StructField("num_ligne_tc", StringType(), True),
        StructField("type_voiture", StringType(), True),
        StructField("service_agent_id", StringType(), True),
        StructField("service_agent_lib", StringType(), True),
        StructField("service_agent_group", StringType(), True),
        StructField("type_heure_id", StringType(), True),
        StructField("type_heure_lib", StringType(), True),
        StructField("groupe_id", StringType(), True),
        StructField("groupe_lib", StringType(), True),
        StructField("sd_id", StringType(), True),
        StructField("sd_lib", StringType(), True),
        StructField("status_absense", StringType(), True),
        StructField("service_agent_type", StringType(), True),
        StructField("heures_plateforme", StringType(), True),
        StructField("nom_jour", StringType(), True),
        StructField("code_ss_groupe", StringType(), True),
        StructField("libelle_ss_groupe", StringType(), True),
        StructField("code_trame", StringType(), True),
        StructField("code_unite_actuelle", StringType(), True),
        StructField("code_ss_groupe_vacation", StringType(), True),
        StructField("libelle_ss_groupe_vacation", StringType(), True),
        StructField("lieu_debut_service", StringType(), True),
        StructField("num_voiture", StringType(), True),
        StructField("lieu_fin_service", StringType(), True),
        StructField("mode_exploitation", StringType(), True),
        StructField("service_agent_origine", StringType(), True),
        StructField("libelle_heure", StringType(), True),
        StructField("libelle_groupe", StringType(), True),
        StructField("heures_travaillees", StringType(), True),
        StructField("heures_travaillees_reelles", StringType(), True),
        StructField("heures_payees", StringType(), True),
        StructField("heures_conduite", StringType(), True),
        StructField("majoration", DoubleType(), True),
        StructField("id_absence", StringType(), True),
        StructField("type_absence", StringType(), True)
])

service = read_csv("services.csv")

dformat = "yyyy-MM-dd"
if filiale_name=="autocarPlanche":
  dformat = "dd/MM/yyyy"
  
SER = service.alias('SER')
SER = SER.withColumn("date_roulement", F.to_date(F.col("date_roulement"), dformat))\
         .withColumn("majorations", F.regexp_replace('majorations', ',', '.').cast("double"))

SER = SER.fillna(0, subset=["majorations"])
ss = SER.select("service_agent_id", "service_agent_lib").filter(F.col("service_agent_id").startswith("MA")).drop_duplicates()
display(ss)
#################################################################################################

# Assurer la densité de SER sur l'index id_salarie, date_roulement ##############################

# df1 = SER.groupBy("id_salarie").agg(F.min("date_roulement").alias("date_roulement")).toPandas() 
# df2 = SER.groupBy("id_salarie").agg(F.max("date_roulement").alias("date_roulement")).toPandas() 
# df_orig = SER.select("id_salarie", "date_roulement").toPandas() 
# df_orig["date_roulement"] = pd.to_datetime(df_orig['date_roulement']).dt.strftime('%Y-%m-%d')

# df = pd.concat([df1, df2], ignore_index=True)
# df = df.sort_values(["id_salarie", "date_roulement"])
# df["date_roulement"] = pd.to_datetime(df["date_roulement"])
# df_alldates = df.set_index('date_roulement').groupby('id_salarie').resample('1D').asfreq().drop(['id_salarie'], 1).reset_index()
# df_alldates['date_roulement'] = df_alldates['date_roulement'].dt.strftime('%Y-%m-%d')

# set_orig = set(df_orig.itertuples(index=False, name=None))
# set_alldates = set(df_alldates.itertuples(index=False, name=None))
# set_missing = set_alldates-set_orig
# rdd_schema = StructType([StructField('id_salarie2', StringType(), True), \
#                          StructField('date_roulement2', StringType(), True)])

# SER_DENSE_IDX = spark.createDataFrame(set_missing, rdd_schema)
# SER_DENSE_IDX = SER_DENSE_IDX.withColumn("date_roulement2", F.to_date(F.col("date_roulement2"),"yyyy-MM-dd"))

# DDF1 = SER.alias('DDF1')
# DDF2 = SER_DENSE_IDX.alias('DDF2')

# SER = DDF1.join(DDF2, (DDF1.id_salarie==DDF2.id_salarie2) &
#                             (DDF1.date_roulement==DDF2.date_roulement2), how='full')
# SER = SER.withColumn("id_salarie", F.when(F.col("id_salarie").isNull(), F.col("id_salarie2")).otherwise(F.col("id_salarie")))
# SER = SER.withColumn("date_roulement", F.when(F.col("date_roulement").isNull(), F.col("date_roulement2")).otherwise(F.col("date_roulement")))
# SER = SER.withColumn("service_agent_id", F.when(F.col("date_roulement2").isNotNull(), F.lit("missing")).otherwise(F.col("service_agent_id")))
# SER = SER.withColumn("type_heure_id", F.when(F.col("date_roulement2").isNotNull(), F.lit("missing")).otherwise(F.col("type_heure_id")))
# SER = SER.drop("id_salarie2", "date_roulement2")
if filiale_name == "besancon":
  SER = SER.filter(F.col("date_roulement") > F.lit('2018-01-01'))
  
if filiale_name == "aerolis":
  SER = SER.filter(F.col("date_roulement") < F.lit('2019-05-01'))


SER = SER.filter(F.col("date_roulement") < F.lit('2019-07-01')).cache()
if type_heure == "float":
  SER = SER.withColumn("heure_debut_service", F.regexp_replace("heure_debut_service", ',', '.').cast("double"))
  SER = SER.withColumn("heure_fin_service", F.regexp_replace("heure_fin_service", ',', '.').cast("double"))

  SER = SER.withColumn("heure_debut_service", F.when(~F.col("heure_debut_service").isNull(), 
                                                                       F.col("heure_debut_service")).otherwise(0))

  SER = SER.withColumn("heure_fin_service", F.when(~F.col("heure_fin_service").isNull(), 
                                                                     F.col("heure_fin_service")).otherwise(0))

  SER = SER.withColumn("heure_debut_service_hh", F.unix_timestamp(F.col('date_roulement')).cast("integer") + (F.col("heure_debut_service") * 3600).cast("integer"))  
  SER = SER.withColumn("heure_debut_service_hh", F.from_unixtime("heure_debut_service_hh", format = 'HH:mm'))  
  SER = SER.drop("heure_debut_service").withColumnRenamed("heure_debut_service_hh","heure_debut_service")

  SER = SER.withColumn("heure_fin_service_hh", F.unix_timestamp(F.col('date_roulement')).cast("integer") + (F.col("heure_fin_service") * 3600).cast("integer"))  
  SER = SER.withColumn("heure_fin_service_hh", F.from_unixtime("heure_fin_service_hh", format = 'HH:mm'))  
  SER = SER.drop("heure_fin_service").withColumnRenamed("heure_fin_service_hh", "heure_fin_service")

if type_heure == "mktime":
  SER = SER.withColumn("heure_debut_service_hh", F.unix_timestamp(F.col('date_roulement')).cast("integer") + F.col("heure_debut_service").cast("integer"))
  SER = SER.withColumn("heure_debut_service_hh", F.from_unixtime("heure_debut_service_hh", format = 'HH:mm'))  
  SER = SER.drop("heure_debut_service").withColumnRenamed("heure_debut_service_hh","heure_debut_service")

  SER = SER.withColumn("heure_fin_service_hh", F.unix_timestamp(F.col('date_roulement')).cast("integer") + F.col("heure_fin_service").cast("integer"))  
  SER = SER.withColumn("heure_fin_service_hh", F.from_unixtime("heure_fin_service_hh", format = 'HH:mm'))  
  SER = SER.drop("heure_fin_service").withColumnRenamed("heure_fin_service_hh", "heure_fin_service")
#################################################################################################

# COMMAND ----------

# DBTITLE 1,Définition des valeurs des champs
#Liste des données pouvant être présentes dans les champs des objets. Les champs pouvant prendre différentes valeurs cette partie est la plus importante de TOUT le code, il permet de faire le lien entre les différentes données issues de toutes les origines possibles.
#En cas d'ajout de filiales ou d'origines de données cette liste doit être complété. 

#SERVICE #####################################################################################
#MAL = Maladie | ACC = Accident de travail / Accident de trajet | NAU = Non autorisé 
#service_agent_id 
#############[###################TRANSPOLE#####################][######PLANCHE######]
SER_SA_MAL = ["AAI","AIM","AIMA","MRN","FMA","MAL","MMA","MRTT", "CM", "CMR", 'CMRH']
#############[#######TRANSPOLE#######][#####PLANCHE#####]
SER_SA_ACC = ["ACC","ATF","MMB","RACC", "AT", "ATT", "ATR","ATRH"]
#################[PLANCHE]
# SER_SA_NAU = ['DAE', 'CSS', 'AAP', 'ABS', 'ANANP', 'AANP']
SER_SA_NAU = ["ANANP", "ABS", "CSS", "MAP"]

#type_heure_id 
#############[#################TRANSPOLE###########################]
SER_TH_MAL = ["49","AAI","AIM","AIMA","MRN","FMA","MAL","MMA","MRTT"]
#############[#######TRANSPOLE#######]
SER_TH_ACC = ["ACC","ATF","MMB","RACC"]

#id_absence 
###########[BESANCON]
SER_AB_MAL = ["MAL"]
###########[BESANCON]
SER_AB_ACC = ["AT","ATJ"]
###########[BESANCON]
SER_AB_NAU = ["ANNP"]


#############[###################TRANSPOLE####################]
TH_TRAVAIL = ["01", "05", "HED"] 
###########[BESANCON]
SA_GROUP = ["DUTY"]

###############################################################################################

#SALARIE ######################################################################################

#situation_familiale 
#########[###############TRANSPOLE###################][############BESANCON#############][###PLANCHE###]
SAL_SF = ["PACS","MARIE","VIE_MARITALE","CONCUBINAGE","Vie maritale", "Marié(e)", "PACS", "1", "5", "6"]

#sexe
######[TRANSPOLE][BESANCON][PLANCHE]
SAL_SE_H = ["Masculin", "M", "1"]
######[TRANSPOLE][BESANCON][PLANCHE]
SAL_SE_F = ["Féminin", "F", "2"]

###############################################################################################

#CONTRAT ######################################################################################

#lib_categorie_professionnellere
#########[##############################TRANSPOLE#################################]
CON_CP = get_col_unique(CON, "lib_categorie_professionnelle")
# ["MAITRISE", "EMPLOYE", "HAUTE MAITRISE", "OUVRIER", "CONDUCTEUR", "CADRE"]

#nature_contrat
#CPS => conducteur periode scolaire
########[TRANSPOLE, BESANCON, PLANCHE]##
CON_NC_CDI = ["CDI"]
#####[TRANSPOLE, BESANCON][PLANCHE]
CON_NC_CDD = ["CDD", "CDDA"]
###########[TRANSPOLE][PLANCHE]
CON_NC_STA = ["STAG",  "CPROF", "CAP10"]
#############[TRANSPOLE]##
CON_NC_VAC = ["CDD_VAC"]
#############[PLANCHE]##
CON_NC_CPS = ["CPS"]

###############################################################################################



# COMMAND ----------

# DBTITLE 1,Gestion du calendrier (jours fériés, vacances, météo, évenements internes etc.)
CAL_VAR = SER


# Import fichier jour_ferie, vacances scolaires, épidémie grippe et gastro, fetes religieuses #############################################################################################

calendrierSchema = StructType([
        StructField("date", DateType(), True),
        StructField("grippe", StringType(), True),
        StructField("gastro", StringType(), True),
        StructField("is_ferie", IntegerType(), True),
        StructField("lib_ferie", StringType(), True),
        StructField("vacance", IntegerType(), True),
        StructField("zoneA", IntegerType(), True),
        StructField("zoneB", IntegerType(), True),  
        StructField("zoneC", IntegerType(), True),
        StructField("lib_vacances", StringType(), True),
        StructField("fete_religion", IntegerType(), True),
        StructField("lib_fete_religion", StringType(), True)  
])

calendrier = read_csv("calendrier.csv", calendrierSchema)
calendrier = calendrier.withColumn('grippe', F.regexp_replace('grippe', ',', '.').cast("double"))\
                       .withColumn('gastro', F.regexp_replace('gastro', ',', '.').cast("double"))\
                       .withColumn("date",F.to_date(F.col("date"),"yyyy-MM-dd"))

###########################################################################################################################################################################################

# Import fichier meteo ####################################################################################################################################################################

meteo = read_csv("meteo.csv")
meteo = meteo.withColumn('temperatureHigh', F.col('temperatureHigh').cast("double"))\
        .withColumn('temperatureLow', F.col('temperatureLow').cast("double"))\
        .withColumn('apparentTemperatureHigh', F.col('apparentTemperatureHigh').cast("double"))\
        .withColumn('apparentTemperatureLow', F.col('apparentTemperatureLow').cast("double"))\
        .withColumn('dewPoint', F.col('dewPoint').cast("double"))\
        .withColumn('windSpeed', F.col('windSpeed').cast("double"))\
        .withColumn('humidity', F.col('humidity').cast("double"))\
        .withColumn('precipIntensity', F.col('precipIntensity').cast("double"))\
        .withColumn('precipAccumulation', F.col('precipAccumulation').cast("double"))\
        .withColumn("date",F.to_date(F.col("date"),"dd/MM/yyyy"))

###########################################################################################################################################################################################

#Jointure entre le fichier des variables calculées (roulements) et [calendrier, météo] ####################################################################################################

#Jointure entre le fichier des variables calculées et les jours particuliers -> pour les interactions de surlignages temporels 
ag_roul2 = CAL_VAR.alias('ag_roul2')
cal2 = calendrier.alias('cal2')
CAL_VAR = ag_roul2.join(cal2,ag_roul2.date_roulement==cal2.date, how='left')\
  .select('ag_roul2.*','cal2.grippe', 'cal2.gastro','cal2.is_ferie','cal2.lib_ferie', 'cal2.fete_religion', 'cal2.lib_fete_religion','cal2.zoneA','cal2.zoneB','cal2.zoneC')

#Jointure entre le fichier des variables calculées et la meteo -> pour les interactions de surlignages temporels
ag_roul3 = CAL_VAR.alias('ag_roul3')
met = meteo.alias('met')
CAL_VAR = ag_roul3.join(met,ag_roul3.date_roulement==met.date, how='left')\
  .select('ag_roul3.*','met.precipType')
###########################################################################################################################################################################################

#Création des WINDOWS pour les jours "spéciaux" (vacances, jours fériés, épidémiologie etc.) ##############################################################################################

#Création variable Jour_Ferie_2J & Jour_Religieux_2J (+ ou - 2) jours

windowsSpec = Window.orderBy("date").rowsBetween(-2,2)
calendrier = calendrier.withColumn("Jour_Ferie_2J",F.when((F.sum(F.when(F.col("is_ferie")==1,1).otherwise(0)).over(windowsSpec)>0) ,1).otherwise(0))\
                       .withColumn("Jour_Religieux_2J",F.when(F.sum(F.when(F.col("fete_religion")==1,1).otherwise(0)).over(windowsSpec)>0,1).otherwise(0))\
                       .withColumn("Jour_Religieux", F.col("fete_religion"))\
                       .withColumn("Jour_Ferie", F.col("is_ferie"))

#Création variables Periode_Grippe & Periode_Gastro (quantile 90)

Q90_grippe = calendrier.approxQuantile("grippe", [0.9],0) # [21.0]
Q90_gastro = calendrier.approxQuantile("gastro", [0.9],0) # [127.0]
calendrier = calendrier.withColumn("Periode_Grippe", F.when(F.col("grippe")>= Q90_grippe[0],1).otherwise(0))\
                       .withColumn("Periode_Gastro", F.when(F.col("gastro")>= Q90_gastro[0],1).otherwise(0))\
                       .withColumn("Periode_Epidemie", F.when( ((F.col("Periode_Gastro")==1) | (F.col("Periode_Grippe")==1)) ,1).otherwise(0))

#Création variable Periode_vacances_scolaires (+ ou - 2 jours)

calendrier = calendrier.withColumn("Periode_vacances_scolaires", F.when(F.col("lib_vacances").isNotNull() & ((F.col(zone_vac) == 1)), 1).otherwise(0))

# calendrier = calendrier.withColumn("lib_vacances_annee", F.when((F.col(zone_vac) == 1), F.concat(F.col('lib_vacances'),F.lit('-'), F.year('date'))).otherwise(""))
# calendrier = calendrier.withColumn("lib_vacances_annee", F.when((calendrier.lib_vacances_annee.like('%No??l%')) & (F.month(F.col("date"))==1) , F.concat(F.col('lib_vacances'), F.lit('-'), F.year('date')-1)).otherwise(F.col("lib_vacances_annee")))

# windowsSpec = Window.orderBy(zone_vac, "lib_vacances_annee", "date").partitionBy(zone_vac, "lib_vacances_annee")
# windowsSpecTot = Window.orderBy("lib_vacances_annee","date")

# calendrier = calendrier.withColumn("lib_vacances_annee_prev",F.lag(calendrier.lib_vacances_annee).over(windowsSpec))\
#                        .withColumn("ID_VAC", F.sum( F.when(((F.col("lib_vacances_annee_prev") == F.col("lib_vacances_annee")) |(F.col("lib_vacances_annee").isNull()) | (F.col(zone_vac) == 0)),0).otherwise(1))\
#                        .over(windowsSpecTot))\
#                        .drop(*['lib_vacances_annee_prev','lib_vacances_annee'])



# windowsSpec = Window.partitionBy("ID_VAC").orderBy("ID_VAC", "date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# calendrier = calendrier.withColumn("DATE_VAC_DEBUT", F.first(F.col("date")).over(windowsSpec))\
#                        .withColumn("DATE_VAC_FIN", F.last(F.col("date")).over(windowsSpec))

# display(calendrier.filter((F.col("ID_VAC")==90)))

windowsSpec = Window.orderBy("date").rowsBetween(-2,2)

# calendrier = calendrier.withColumn("Periode_vacances_scolaires", F.when(((F.sum(F.col("ID_VAC")).over(windowsSpec) >0) & (F.col("ID_VAC")==0)),1).otherwise(0)) 

# calendrier = calendrier.withColumn("Periode_vacances_scolaires", F.when(F.col("DATE_VAC_DEBUT")==F.col("date"), 1).otherwise(
#                                                                         F.when(F.col("DATE_VAC_FIN")==F.col("date")), 0).otherwise(None))



#Création variable Periode_neige 

meteo = meteo.withColumn("FLAG_SNOW",F.when(F.col("precipType") == "snow",1).otherwise(0)).withColumn("Periode_neige", F.when((F.sum(F.col("FLAG_SNOW")).over(windowsSpec) >0) ,1).otherwise(0)) 

#Jointure avec les agents_roulements

cal2 = calendrier.alias('cal2')
met2 = meteo.alias('met2')
# import time
# time.sleep(10000)
cal2 = cal2.join(met2, cal2.date == met2.date, how='left')\
  .select('cal2.date', 'cal2.Jour_Ferie', 'cal2.Jour_Religieux', 'cal2.Jour_Ferie_2J', 'cal2.Jour_Religieux_2J', 'cal2.Periode_Grippe', 'cal2.Periode_Gastro', 'cal2.Periode_Epidemie', 'cal2.Periode_vacances_scolaires', 'met2.Periode_neige').withColumn('Periode_neige', F.when(F.col('Periode_neige').isNull(),0).otherwise(F.col('Periode_neige')))
#  
cal2 = cal2.withColumn('Jour_Ferie_2J', F.when(F.col('Jour_Ferie') > 0, 100).otherwise(F.col('Jour_Ferie_2J')))
cal2 = cal2.withColumn('Jour_Religieux_2J', F.when(F.col('Jour_Religieux') > 0, 100).otherwise("Jour_Religieux_2J"))
#Création variable Periode_temperatures_extremes 

meteo = meteo.withColumn("Periode_temperatures_extremes",F.when((F.col("temperatureHigh") > 30) | (F.col("temperatureLow") < 0) ,1).otherwise(0))\

#Jointure avec les agents_roulements

# ag_roul_id31 = CAL_VAR.alias('ag_roul_id31')
cal3 = cal2.alias('cal3')
met3 = meteo.alias('met3')

cal3 = cal3.join(met3, cal3.date == met3.date, how='left')\
                      .select('cal3.*','met3.Periode_temperatures_extremes')\
                      .withColumn('Periode_temperatures_extremes', F.when(F.col('Periode_temperatures_extremes').isNull(),0).otherwise(F.col('Periode_temperatures_extremes')))

cal3 = cal3.withColumn('Weekend', F.when(date_format('date', 'u').isin(["6","7"]),1).otherwise(0))\
                 .withColumn('Jour_ouvre', F.when(date_format('date', 'u').isin(["1","2","3","4","5"]),1).otherwise(0))\
                 .withColumn('Vendredi', F.when(date_format('date', 'u').isin(["5"]),1).otherwise(0))\
                 .withColumn('Lundi', F.when(date_format('date', 'u').isin(["1"]),1).otherwise(0))\

#Jointure avec les agents_roulements

ag_roul_id2 = CAL_VAR.alias('ag_roul_id2')
cal4 = cal3.alias('cal4')
CAL_VAR = ag_roul_id2.join(cal4, ag_roul_id2.date_roulement == cal4.date, how='left')\
  .select('ag_roul_id2.*', 'cal4.*')

max_date = CAL_VAR.select("date_roulement").rdd.max()[0]
min_date = CAL_VAR.select("date_roulement").rdd.min()[0]

cal3 = cal3.filter((F.col("date") >= min_date) & (F.col("date") <= max_date))

#export_result_df(CAL_VAR, 'CAL_VAR.csv', True)
CAL_VAR_LIST = ["Jour_Ferie_2J", "Jour_Religieux_2J", "Periode_Grippe", "Periode_Gastro", "Periode_Epidemie", "Periode_vacances_scolaires", "Periode_neige", "Periode_temperatures_extremes", "Weekend", "Jour_ouvre", "Vendredi", "Lundi"]

cal3 = cal3.withColumn("alldays", F.lit(1))

cal3 = cal3.withColumn('sum', sum(cal3[col] for col in CAL_VAR_LIST + ["alldays"]))
# for v in CAL_VAR_LIST:
#   cal3 = cal3.withColumn(v, F.when(F.col(v)==1, v).otherwise(None))

from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable 
def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

cal4 = melt(cal3, ["date", "sum"], CAL_VAR_LIST + ["alldays"])
export_result_df(cal4, 'calendrier.csv')


# COMMAND ----------

# CAL_VAR = CAL_VAR.filter((F.col("date_roulement")>'2018-06-03') & (F.col("date_roulement")<'2018-06-25')).cache()

# COMMAND ----------

# DBTITLE 1,Calcul des variables sur l'objet service - Durée d'absence
#CALCUL SI ABSENT (maladie, accident du travail, absence non justifiée) #####################################################
#Restriction aux roulements de type "absences". 
#Séparation en plusieurs catégorie : 
#- Accident du travail
#- Maladie
#- Absence non justifié

SER_WITH_ABS = CAL_VAR.withColumn("FLAG_ABS", F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_MAL+SER_SA_ACC+SER_SA_NAU) | F.trim(F.col("type_heure_id")).isin(SER_TH_MAL+SER_TH_ACC) | F.trim(F.col("id_absence")).isin(SER_AB_MAL+SER_AB_ACC+SER_AB_NAU), 1).otherwise(0))\
         .withColumn("TYPE_ABS", F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_MAL) | F.trim(F.col("type_heure_id")).isin(SER_TH_MAL) | F.trim(F.col("id_absence")).isin(SER_AB_MAL), "MALADIE")\
               .otherwise(F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_ACC) | F.trim(F.col("type_heure_id")).isin(SER_TH_ACC) | F.trim(F.col("id_absence")).isin(SER_AB_ACC), "ACCIDENT")\
               .otherwise(F.when(F.trim(F.col("id_absence")).isin(SER_AB_NAU), "NON AUTORISE")\
               .otherwise(F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_NAU), "AUTRE")\
                          .otherwise("")))))\
         .withColumn("service_agent_with_abs", F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_MAL) | F.trim(F.col("type_heure_id")).isin(SER_TH_MAL) | F.trim(F.col("id_absence")).isin(SER_AB_MAL), "MAL")\
               .otherwise(F.when(F.trim(F.col("service_agent_id")).isin(SER_SA_ACC) | F.trim(F.col("type_heure_id")).isin(SER_TH_ACC) | F.trim(F.col("id_absence")).isin(SER_AB_ACC), "ACC")\
               .otherwise(F.when(F.trim(F.col("id_absence")).isin(SER_AB_NAU), "NAU")\
               .otherwise(F.trim(F.col("service_agent_id"))))))\
         .withColumn("FLAG_SER_TRAV", F.when(F.trim(F.col("type_heure_id")).isin(TH_TRAVAIL) | F.trim(F.col("service_agent_group")).isin(SA_GROUP), 1).otherwise(0))

SER_WITH_ABS = SER_WITH_ABS.withColumn("FLAG_ABS_RAW", F.col("FLAG_ABS"))
SER_WITH_ABS = SER_WITH_ABS.select("id_salarie", "date_roulement", "heure_debut_service", "heure_fin_service", "FLAG_ABS", "FLAG_ABS_RAW", "FLAG_SER_TRAV", "TYPE_ABS", "service_agent_id", "service_agent_with_abs", "service_agent_origine", "lieu_debut_service", "num_ligne_tc", "type_voiture", "num_voiture", "sd_id", "code_ss_groupe_vacation", "code_unite_actuelle", *CAL_VAR_LIST, 'Jour_Ferie', 'Jour_Religieux', "type_absence", "majorations").distinct()

# display(SER_WITH_ABS.filter((F.col("FLAG_ABS")>=0) &\
#                             ((F.col("date_roulement")>'2018-06-03') & (F.col("date_roulement")<'2018-06-25')) &\
#                             (F.col("id_salarie")=='2189716')))

if filiale_name=="aerolis":
  SER_WITH_ABS = SER_WITH_ABS.withColumn("type_voiture", F.lit("donnee manquante"))

##############################################################################################################################
#FLAG_ABS doit etre egal à 1 pour tout WE ou JF  compris entre 2 perioes d'absence ###########################################
SER_WITH_ABS = SER_WITH_ABS.withColumn("flag_abs_ffill", F.when(((F.col("Weekend")==1) | (F.col("Jour_Ferie")==1)) & (F.col("FLAG_ABS")==0), 
                                                                F.lit(None).cast(IntegerType())).otherwise(F.col("FLAG_ABS")))\
                           .withColumn("flag_abs_bfill", F.col("flag_abs_ffill"))

SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "flag_abs_ffill", "id_salarie", "date_roulement", True)
SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "flag_abs_bfill", "id_salarie", "date_roulement", False)
SER_WITH_ABS = SER_WITH_ABS.withColumn("flag_abs_bf", F.col("flag_abs_ffill") +  F.col("flag_abs_bfill"))   
SER_WITH_ABS = SER_WITH_ABS.withColumn("FLAG_ABS", F.when(F.col("flag_abs_bf")==2, 1).otherwise(F.col("FLAG_ABS")))                                  
SER_WITH_ABS = SER_WITH_ABS.drop("flag_abs_bfill", "flag_abs_ffill", "flag_abs_bf")

def fill_holes_between_abs(df, col):
  col_ff = "{}_ffill".format(col)
  col_bf = "{}_bfill".format(col)

  df = df.withColumn(col_ff, F.when(((F.col("Weekend")==1) | (F.col("Jour_Ferie")==1)) & (F.col("FLAG_ABS_RAW")==0), 
                                                                F.lit(None).cast(StringType())).otherwise(F.col(col)))\
                           .withColumn(col_bf, F.col(col_ff))

  df = fill_nulls(df, col_ff, "id_salarie", "date_roulement", True)
  df = fill_nulls(df, col_bf, "id_salarie", "date_roulement", False)
  df = df.withColumn(col, F.when((F.col(col_ff)==F.col(col_bf)) & 
                                                              (F.col("FLAG_ABS")==1),
                                                              F.col(col_ff))\
                                                               .otherwise(F.col(col)))                                  
  df = df.drop(col_ff, col_bf)
  return df

SER_WITH_ABS = fill_holes_between_abs(SER_WITH_ABS, 'service_agent_with_abs')
SER_WITH_ABS = fill_holes_between_abs(SER_WITH_ABS, 'service_agent_id')
SER_WITH_ABS = fill_holes_between_abs(SER_WITH_ABS, 'TYPE_ABS')


# #Idem avec service_agent_with_abs pour tout WE ou JF  compris entre 2 perioes d'absence de meme type #########################
# SER_WITH_ABS = SER_WITH_ABS.withColumn("service_agent_with_abs_ffill", F.when((F.col("Weekend")==1) | (F.col("Jour_Ferie")==1), 
#                                                                 F.lit(None).cast(StringType())).otherwise(F.col("service_agent_with_abs")))\
#                            .withColumn("service_agent_with_abs_bfill", F.col("service_agent_with_abs_ffill"))

# SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "service_agent_with_abs_ffill", "id_salarie", "date_roulement", True)
# SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "service_agent_with_abs_bfill", "id_salarie", "date_roulement", False)
# SER_WITH_ABS = SER_WITH_ABS.withColumn("service_agent_with_abs", F.when((F.col("service_agent_with_abs_ffill")==F.col("service_agent_with_abs_bfill")) & 
#                                                             (F.col("FLAG_ABS")==1),
#                                                             F.col("service_agent_with_abs_ffill"))\
#                                                              .otherwise(F.col("service_agent_with_abs")))                                  
# SER_WITH_ABS = SER_WITH_ABS.drop("service_agent_with_abs_ffill", "service_agent_with_abs_bfill")



# #Idem avec service_agent_with_abs pour tout WE ou JF  compris entre 2 perioes d'absence de meme type #########################
# SER_WITH_ABS = SER_WITH_ABS.withColumn("service_agent_id_ffill", F.when((F.col("Weekend")==1) | (F.col("Jour_Ferie")==1), 
#                                                                 F.lit(None).cast(StringType())).otherwise(F.col("service_agent_id")))\
#                            .withColumn("service_agent_id_bfill", F.col("service_agent_id_ffill"))

# SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "service_agent_id_ffill", "id_salarie", "date_roulement", True)
# SER_WITH_ABS = fill_nulls(SER_WITH_ABS, "service_agent_id_bfill", "id_salarie", "date_roulement", False)
# SER_WITH_ABS = SER_WITH_ABS.withColumn("service_agent_id", F.when((F.col("service_agent_id_ffill")==F.col("service_agent_id_bfill")) & 
#                                                             (F.col("FLAG_ABS")==1),
#                                                             F.col("service_agent_id_ffill"))\
#                                                              .otherwise(F.col("service_agent_id")))                                  
# SER_WITH_ABS = SER_WITH_ABS.drop("service_agent_id_ffill", "service_agent_id_bfill")

# display(SER_WITH_ABS.orderBy("date_roulement").select("date_roulement", "service_agent_id", "FLAG_ABS", "Weekend", "Jour_Ferie"))

# display(SER_WITH_ABS.filter(F.col("id_salarie")=="6648634"))

#export_result_df(SER_WITH_ABS, 'SER_WITH_ABS.csv', True)
##############################################################################################################################

#Création d'un identifiant ID_SERVICE_ABSENCE ################################################################################

windowService = Window.partitionBy("id_salarie").orderBy("id_salarie", "date_roulement", "heure_debut_service")

SER_WITH_ABS = SER_WITH_ABS\
                    .withColumn("date_roulement_PREV",F.lag(SER_WITH_ABS.date_roulement).over(windowService))\
                    .withColumn("service_agent_with_abs_PREV", F.lag(SER_WITH_ABS.service_agent_with_abs).over(windowService))

SER_WITH_ABS = SER_WITH_ABS\
                    .withColumn("DIFF_DATE", F.datediff(SER_WITH_ABS.date_roulement,SER_WITH_ABS.date_roulement_PREV))\
                    .withColumn("ID_SERVICE_ABSENCE", F.sum( F.when(((F.col("DIFF_DATE") == 1) & (F.col("service_agent_with_abs") == F.col("service_agent_with_abs_PREV")) & (F.col("FLAG_ABS")==1)) | 
                                                                    ((F.col("DIFF_DATE") < 1) & (F.col("service_agent_with_abs") == F.col("service_agent_with_abs_PREV"))), 0)\
                                                             .otherwise(1)).over(windowService))\
                    .drop(*['date_roulement_PREV','service_agent_with_abs_PREV'])

##############################################################################################################################

#Calcul des tranches de durées d'absence #####################################################################################

#Création d'une DATE_DEBUT et DATE_FIN pour pour récupérer la durée des roulement (et donc des absences)
windowService = Window.partitionBy("id_salarie","ID_SERVICE_ABSENCE").orderBy("id_salarie","ID_SERVICE_ABSENCE","date_roulement").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

SER_WITH_ABS_ID = SER_WITH_ABS\
                    .withColumn("DATE_DEBUT", F.first(F.col("date_roulement")).over(windowService))\
                    .withColumn("DATE_FIN", F.last(F.col("date_roulement")).over(windowService))

SER_WITH_ABS_ID = SER_WITH_ABS_ID.withColumn('duree_jours', F.datediff("DATE_FIN","DATE_DEBUT")+1)

#Association des tranches de durées d'absence à partir du nombre de jour d'absence (permet la mise en place de filtres pour la dataviz)
windowServiceAbsence = (Window.partitionBy("id_salarie", 'ID_SERVICE_ABSENCE').orderBy('date_roulement').rangeBetween(-sys.maxsize, 0))

SER_ABS = SER_WITH_ABS_ID.withColumn('duree_cumul', F.sum('DIFF_DATE').over(windowServiceAbsence))
SER_ABS = SER_ABS.drop(*["DIFF_DATE"])

SER_ABS = SER_ABS.withColumn("type_duree",F.when((F.col("duree_jours")<=1) & (F.col("FLAG_ABS")==1),"1 jour")\
                                        .otherwise(F.when((F.col("duree_jours")<=3) & (F.col("FLAG_ABS")==1),"2 à 3 jours")\
                                        .otherwise(F.when((F.col("duree_jours")<=7) & (F.col("FLAG_ABS")==1),"4 à 7 jours")\
                                        .otherwise(F.when((F.col("duree_jours")<=31) & (F.col("FLAG_ABS")==1),"8 jours à 1 mois")\
                                        .otherwise(F.when((F.col("duree_jours")<=93) & (F.col("FLAG_ABS")==1),"1 à 3 mois")\
                                        .otherwise(F.when((F.col("duree_jours")>93) & (F.col("FLAG_ABS")==1),"plus de 3 mois")\
                                        .otherwise("non_absent")))))))

# Calcul de l'effectif réalisé pour le calcul du taux d'absentéisme
SER_CALC_EFF = SER_ABS.groupBy("date_roulement").agg(F.countDistinct(F.col("id_salarie")))\
                               .withColumnRenamed('count(DISTINCT id_salarie)', 'effectif_reel')\
                               .withColumnRenamed('date_roulement', 'date_roulement2')

SER_EFF = SER_CALC_EFF.alias('SER_EFF')
SER_CALC_ABS = SER_ABS.alias('SER_CALC_ABS')

SER_CALC_ABS = SER_CALC_ABS.join(SER_EFF, (SER_CALC_ABS.date_roulement == SER_EFF.date_roulement2), how='left').select('SER_CALC_ABS.*','SER_EFF.effectif_reel').distinct()

SER_CALC_ABS = SER_CALC_ABS.withColumn("mois_annee", substring(SER_CALC_ABS['date_roulement'], 0, 7))
# SER_CALC_ABS = bcache(SER_CALC_ABS)
#export_result_df(SER_CALC_ABS, 'SER_WITH_ABS_ID.csv', True)

##############################################################################################################################

# COMMAND ----------

# DBTITLE 1,Merge et traitement de données sur l'objet service - Dépots et lignes
SER_VAR_AUTRE = SER_CALC_ABS
# Ajout du dépot de début de service sur chaque lignes de l'objet service 

if filiale_name == "transpole":
  DEP2 = DEP.filter(F.col("id_depot")=="1321")
  DEP2 = DEP2.withColumn("id_depot", F.lit("7220"))
  DEP = DEP.union(DEP2)
  DEP = DEP.withColumn("lib_depot_add", F.trim(F.concat(F.col('cp_depot'), F.lit(' '), F.col('adresse_depot'))))
  DEP = DEP.withColumn('lib_depot_add', F.when(F.col("lib_depot_add") == '59200 TOURCOING Rue de Lyser', "59200 TOURCOING Rue de L'Yser").otherwise(F.col("lib_depot_add")))
  DEP = DEP.withColumn('Tramway', F.when(F.col("id_depot").isin(["7220", "1321"]), 1).otherwise(0))
else:
  DEP = DEP.withColumn("lib_depot_add", F.trim(F.concat(F.col('lib_depot'), F.lit(' '), F.col('cp_depot'), F.lit(' '), F.col('adresse_depot'))))
  DEP = DEP.withColumn('Tramway', F.lit(0))
  
if filiale_name == "aerolis":
  serviceBrut = SER_VAR_AUTRE.alias('serviceBrut')
  mtc = CEN.alias('mtc')
  SER_VAR_AUTRE = serviceBrut.join(mtc, ((serviceBrut.lieu_debut_service == mtc.ID_CENTRE)), how='left')\
                      .select('serviceBrut.*', 'mtc.LIB_CENTRE', 'mtc.ID_CENTRE', 'mtc.CP')\
                      .withColumnRenamed('LIB_CENTRE', "lib_depot")\
                      .withColumnRenamed('ID_CENTRE', "id_depot")\
                      .withColumnRenamed('CP', "cp_depot")\
                      .withColumn('lib_depot_add', F.col("lib_depot"))

else:
  serviceBrut = SER_VAR_AUTRE.alias('serviceBrut')
  mtc = DEP.alias('mtc')
  SER_VAR_AUTRE = serviceBrut.join(mtc, ((serviceBrut.sd_id == mtc.id_depot) | (serviceBrut.lieu_debut_service == mtc.id_depot)), how='left')\
                      .select('serviceBrut.*','mtc.lib_depot', 'mtc.lib_depot_add', 'mtc.id_depot', 'mtc.cp_depot', "mtc.Tramway")
  

DEP_VAR = get_col_unique(SER_VAR_AUTRE, "lib_depot_add")
# Ajout de la ligne liée au service sur chaque lignes de l'objet service

LIG_VAR = LIG
# display(LIG)
serviceBrut = SER_VAR_AUTRE.alias('serviceBrut')
mtc = LIG_VAR.alias('mtc')

if filiale_name == "transpole":
  SER_VAR_DEPLIG = serviceBrut.join(mtc, (((serviceBrut.code_ss_groupe_vacation == mtc.id_ligne_service) &
                     (serviceBrut.code_unite_actuelle == mtc.code_unite) & mtc.est_ligne) | (serviceBrut.num_ligne_tc == mtc.id_ligne_service)), how='left')\
                        .select('serviceBrut.*','mtc.lib_ligne','mtc.id_ligne_service')
else:
  SER_VAR_DEPLIG = serviceBrut.withColumn("lib_ligne", F.lit(None))

SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_ligne", F.when(F.col('FLAG_ABS')==1, None).otherwise(F.col('lib_ligne')))
SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("num_ligne_tc", F.when(F.col('FLAG_ABS')==1, None).otherwise(F.col('num_ligne_tc')))

if filiale_name == "aerolis":
  lignes_aero = ['L1 - ORLY-ETOILE', 'L2 - ETOILE', 'L4 - CDG-MONTPARNASSE', 'L3 - CDG-ORLY']
  SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_ligne", F.when(F.col('num_ligne_tc').isin(lignes_aero), F.col('num_ligne_tc')).otherwise(None))\
                                 .withColumn("num_ligne_tc", F.col("lib_ligne"))
  
SER_VAR_DEPLIG = fill_nulls_2sides(SER_VAR_DEPLIG, "lib_depot")
SER_VAR_DEPLIG = fill_nulls_2sides(SER_VAR_DEPLIG, "lib_ligne")
SER_VAR_DEPLIG = fill_nulls_2sides(SER_VAR_DEPLIG, "lib_depot_add")

SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_ligne", F.when(F.col('lib_ligne').isNull(), F.col("num_ligne_tc")).otherwise(F.col('lib_ligne')))
SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_ligne", F.when(F.col('lib_ligne').isNull(), "donnee manquante").otherwise(F.col('lib_ligne')))

SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_depot", F.when(F.col('lib_depot').isNull(), "donnee manquante").otherwise(F.col('lib_depot')))
SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_depot_add", F.when(F.col('lib_depot_add').isNull(), "donnee manquante").otherwise(F.col('lib_depot_add')))

lignes_fin_service = {'Ligne Corolle': '2019-02-01', 'Ligne 40W': '2019-02-01', 'Ligne Moliere': '2019-03-01'}
for k, v in lignes_fin_service.items():
  SER_VAR_DEPLIG = SER_VAR_DEPLIG.withColumn("lib_ligne", F.when((F.col("lib_ligne")==k) & 
                                                                 (F.col("date_roulement") > F.lit(v)), "donnee manquante").otherwise(F.col("lib_ligne")))


# COMMAND ----------

# DBTITLE 1,Calcul distance domicile-travail
SER_TPS = SER_VAR_DEPLIG
SAL2 = SAL.withColumn("adresse_salarie", F.concat(F.col("cp_salarie"), F.lit(" "), F.lit("FRANCE")))

df2 = SAL2.alias('df2')
df1 = SER_VAR_DEPLIG.alias('df1')
od_salarie_centre = df1.join(df2, (df1.id_salarie==df2.id_salarie), how='left')\
                       .select('df1.id_salarie','df1.id_depot','df1.cp_depot', "df2.adresse_salarie")
od_salarie_centre = od_salarie_centre.withColumn("adresse_depot", F.concat(F.col("cp_depot"), F.lit(" "), F.lit("FRANCE")))
od_salarie_centre = od_salarie_centre.drop_duplicates()
od_salarie_centre = od_salarie_centre.dropna(subset=('adresse_salarie','adresse_depot'))
# display(od_salarie_centre)
matrice_od_salarie = get_matrice_od(od_salarie_centre, False)
matrice_od_salarie = matrice_od_salarie.drop_duplicates(["id_salarie"])

# display(matrice_od_salarie)

roul = SER_TPS.alias('roul')
mod = matrice_od_salarie.alias('mod')
SER_TPS = roul.join(mod, (roul.id_salarie == mod.id_salarie), how='left')\
                      .select('roul.*','mod.distance_m', "mod.duration_s")

duree_p50 = 17 #SER_TPS.approxQuantile("duration_s", [0.5], 0)[0]
SER_TPS = SER_TPS.withColumn("duration_s", F.when(F.col("duration_s").isNull(), duree_p50).otherwise(F.col("duration_s")))
SER_TPS = fill_nulls_2sides(SER_TPS, "duration_s")
SER_TPS = SER_TPS.withColumn("duration_min", F.col("duration_s").cast('integer') / 60)\
                 .withColumn("duration_min", F.col("duration_min").cast('integer'))
# display(agent_roulement_perim_id)
# quantiles = SER_TPS.approxQuantile("duration_min", [0.25, 0.5, 0.75, 0.95], 0) # [15.0, 21.0, 30.0]
# print(quantiles)

SER_TPS = SER_TPS.withColumn("temps_trajet_inf_15min", F.when(F.col("duration_min") <= 15, 1).otherwise(0))\
                 .withColumn("temps_trajet_sup_30min", F.when(F.col("duration_min") > 30, 1).otherwise(0))\
                  .withColumn('Temps_trajet_dom_trav', F.when(F.col("temps_trajet_inf_15min")==1, "inf_15min").otherwise(
                                                     F.when(F.col("temps_trajet_sup_30min")==1, "sup_30min").otherwise("15-30min")))  

# COMMAND ----------

SER_TPS = cache(SER_TPS, "SER_TPS.csv", False)

# COMMAND ----------

# DBTITLE 1,Calcul des variables sur l'objet service - Amplitude de travail
SER_AMPL_TRAV = SER_TPS

my_window = Window.partitionBy("id_salarie", "date_roulement", "ID_SERVICE_ABSENCE")\
                  .orderBy("id_salarie", "date_roulement", "ID_SERVICE_ABSENCE" )\
                  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

#Conversion des dates / heures de début et de fin de service en timestamp et calcul de l'amplitude en seconde #####################
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("heure_debut_service", F.when(~F.col("heure_debut_service").isNull(), 
                                                                      F.col("heure_debut_service")).otherwise(0))

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("heure_fin_service", F.when(~F.col("heure_fin_service").isNull(), 
                                                                    F.col("heure_fin_service")).otherwise(0))

#Conversion en timestamp 
timeFormat = "yyyy-MM-dd HH:mm"

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_debut_service", F.unix_timestamp(F.concat(date_format(F.col('date_roulement'), 'yyyy-MM-dd'), F.lit(' '), F.col('heure_debut_service')), format=timeFormat))
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_fin_service", F.unix_timestamp(F.concat(date_format(F.col('date_roulement'), 'yyyy-MM-dd'), F.lit(' '), F.col('heure_fin_service')), format=timeFormat))

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_debut_service", F.when(~F.col("timestamp_debut_service").isNull(), 
                                                                            F.col("timestamp_debut_service")).otherwise(0))

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_fin_service", F.when(~F.col("timestamp_fin_service").isNull(), 
                                                                          F.col("timestamp_fin_service")).otherwise(0))

# si heure de fin de service < 4h => on suppose que c'etait le lendemain
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("hfin", F.from_unixtime(F.col("timestamp_fin_service").cast('integer'), format = 'HH').cast('integer'))
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("hdeb", F.from_unixtime(F.col("timestamp_debut_service").cast('integer'), format = 'HH').cast('integer'))

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_fin_service", F.when(F.col("hfin") < 4, F.col("timestamp_fin_service") + 24*60*60).otherwise(F.col("timestamp_fin_service")))
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("timestamp_debut_service", F.when((F.col("hfin") < 4) & (F.col("hdeb") < 4), F.col("timestamp_fin_service") + 24*60*60).otherwise(F.col("timestamp_debut_service")))

#Calcul des heures de service

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("nb_h_service", F.when((F.col("timestamp_fin_service") >= F.col("timestamp_debut_service")), 
                                                               F.col("timestamp_fin_service") - F.col("timestamp_debut_service")).otherwise(
                                                               F.col("timestamp_fin_service") + (60*60*24) - F.col("timestamp_debut_service")))

SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("nb_h_service", F.round(F.col("nb_h_service")/3600).cast('integer'))
if filiale_name in ["besancon", "transpole"]:
  SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("nb_h_service", F.when(F.col("FLAG_SER_TRAV")==1, F.col("nb_h_service")).otherwise(0))

my_window = Window.partitionBy("id_salarie", "date_roulement").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("heures_service", F.sum(F.col("nb_h_service")).over(my_window))
SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("heures_service", F.when((~F.col("heures_service").isNull()), F.col("heures_service").cast('double')).otherwise(0))


###################################################################################################################################

# #########################################################

# display(SER_AMPL_TRAV)
# my_window = Window.partitionBy("id_salarie", "date_roulement").orderBy("id_salarie", "date_roulement", "FLAG_ABS", "nb_h_service").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

subsetUnique = ["date_roulement", "mois_annee" , "id_salarie", "FLAG_ABS", "service_agent_id", "service_agent_with_abs", "service_agent_origine", "ID_SERVICE_ABSENCE", "DATE_DEBUT", "DATE_FIN", "min_heure_debut_service", "max_heure_fin_service", "duree_jours", "type_duree", "effectif_reel"]

SER_AMPL_TRAV = SER_AMPL_TRAV.filter((~F.col("nb_h_service").isNull()) | (F.col("FLAG_ABS")==1))\
    .withColumn("lib_ligne", F.first(F.col("lib_ligne"), True).over(my_window))\
    .withColumn("lib_depot", F.first(F.col("lib_depot"), True).over(my_window))\
    .withColumn("num_ligne_tc", F.first(F.col("num_ligne_tc"), True).over(my_window))\
    .withColumn("lieu_debut_service", F.first(F.col("lieu_debut_service"), True).over(my_window))\
    .withColumn("min_heure_debut_service", F.min(F.col("timestamp_debut_service").cast('integer')).over(my_window))\
    .withColumn("max_heure_fin_service",   F.max(F.col("timestamp_fin_service").cast('integer')).over(my_window))\
    .dropDuplicates(subsetUnique)

SER_AMPL_TRAV = SER_AMPL_TRAV\
      .withColumn("HEUREDEBUT_NUM", F.from_unixtime(F.col("min_heure_debut_service").cast('integer'), format = 'HH').cast('integer'))\
      .withColumn("HEUREFIN_NUM", F.from_unixtime(F.col("max_heure_fin_service").cast('integer'), format = 'HH').cast('integer'))\
      .withColumn("AMPLITUDE_NUM", F.when((F.col("HEUREFIN_NUM") < F.col("HEUREDEBUT_NUM")), F.col("HEUREFIN_NUM") + 24 - F.col("HEUREDEBUT_NUM")).otherwise(F.col("HEUREFIN_NUM") - F.col("HEUREDEBUT_NUM")))\
      .withColumn("AMPLITUDE_NUM", F.col("AMPLITUDE_NUM").cast('integer'))\
      .dropDuplicates(subsetUnique)

if filiale_name == "aerolis":
  SER_AMPL_TRAV = SER_AMPL_TRAV.withColumn("AMPLITUDE_NUM", F.when(F.col("AMPLITUDE_NUM")<=2, F.lit(0)).otherwise(F.col("AMPLITUDE_NUM")))
                                           
# display(SER_AMPL_TRAV.select("HEUREDEBUT_NUM", "HEUREFIN_NUM", "AMPLITUDE_NUM", "id_salarie", "date_roulement", "service_agent_id", 
#                              "num_ligne_tc", "heure_debut_service", "heure_fin_service").filter(F.col("AMPLITUDE_NUM")>0).orderBy("AMPLITUDE_NUM"))

agent_temp = SER_AMPL_TRAV.select(F.col("id_salarie").alias('id_salarie2'),
                                 F.col("date_roulement").alias("date_roulement2"),
                                 F.col("HEUREDEBUT_NUM").alias('HEUREDEBUT_NUM2'),
                                 F.col("HEUREFIN_NUM").alias('HEUREFIN_NUM2'),
                                 F.col("AMPLITUDE_NUM").alias('AMPLITUDE_NUM2'))\
                          .withColumn("dr2", F.unix_timestamp(F.col("date_roulement2")))

NO_H_FILTER = ((agent_temp.HEUREDEBUT_NUM2!=0) | (agent_temp.HEUREFIN_NUM2!=0) | (agent_temp.AMPLITUDE_NUM2!=0))
NO_H_FILTER = ((agent_temp.AMPLITUDE_NUM2!=0))


CFG_AMPL_HOR = {}
CFG_AMP_RATES = {}
if filiale_name == 'transpole':
  CFG_AMPL_HOR = {
    "TRAME": {
      "SOIREE": (agent_temp.HEUREFIN_NUM2 <= 6),
      "JOURNEE": (agent_temp.HEUREFIN_NUM2 > 6),
    },
    "HDEB": { 
      'HEUREDEBUT_TOT': (agent_temp.HEUREDEBUT_NUM2 < 5),  
      'HEUREDEBUT_NORMAL': (agent_temp.HEUREDEBUT_NUM2 >= 5) & (agent_temp.HEUREDEBUT_NUM2 <= 7),
      'HEUREDEBUT_TARD': (agent_temp.HEUREDEBUT_NUM2 > 7)
    },
    "HFIN": {   
      'HEUREFIN_TOT': (agent_temp.HEUREFIN_NUM2 >= 2) & (agent_temp.HEUREFIN_NUM2 <= 20),  
      'HEUREFIN_TARD': (agent_temp.HEUREFIN_NUM2 > 20) & (agent_temp.HEUREFIN_NUM2 < 23),
      'HEUREFIN_TRESTARD': ((agent_temp.HEUREFIN_NUM2 >= 23) | (agent_temp.HEUREFIN_NUM2 <= 2)),
    },
    "AMPL": {
      'AMPLI_FAIBLE': (agent_temp.AMPLITUDE_NUM2 <= 6),  
      'AMPLI_MOYENNE': (agent_temp.AMPLITUDE_NUM2 > 6) & (agent_temp.AMPLITUDE_NUM2 < 12),  
      'AMPLI_GRANDE': (agent_temp.AMPLITUDE_NUM2 >= 12),  
    }
  }
  CFG_AMP_RATES = {
    "PART_SOIREE_HISTO":  {"Bcp_soirees": 0.4},
    "PART_HEUREDEBUT_TOT_HISTO": {"Bcp_debut_service_tot": 0.2},
    "PART_HEUREFIN_TRESTARD_HISTO": {"Bcp_fin_service_tard": 0.4},
    "PART_AMPLI_FAIBLE_HISTO": {"Bcp_faibles_amplitudes": 0.10},
    "PART_AMPLI_GRANDE_HISTO": {"Bcp_grandes_amplitudes": 0.15},
  }
elif filiale_name == 'besancon':
  CFG_AMPL_HOR = {
    "TRAME": {
      "SOIREE": (agent_temp.HEUREFIN_NUM2 <= 6),
      "JOURNEE": (agent_temp.HEUREFIN_NUM2 > 6),
    },
    "HDEB": { 
      'HEUREDEBUT_TOT': (agent_temp.HEUREDEBUT_NUM2 < 6),  
      'HEUREDEBUT_NORMAL': (agent_temp.HEUREDEBUT_NUM2 >= 6) & (agent_temp.HEUREDEBUT_NUM2 <= 9),
      'HEUREDEBUT_TARD': (agent_temp.HEUREDEBUT_NUM2 > 9)
    },
    "HFIN": {   
      'HEUREFIN_TOT': (agent_temp.HEUREFIN_NUM2 >= 2) & (agent_temp.HEUREFIN_NUM2 <= 20),  
      'HEUREFIN_TARD': (agent_temp.HEUREFIN_NUM2 > 20) & (agent_temp.HEUREFIN_NUM2 < 23),
      'HEUREFIN_TRESTARD': ((agent_temp.HEUREFIN_NUM2 >= 23) | (agent_temp.HEUREFIN_NUM2 <= 2)),
    },
    "AMPL": {
      'AMPLI_FAIBLE': (agent_temp.AMPLITUDE_NUM2 <= 7),  
      'AMPLI_MOYENNE': (agent_temp.AMPLITUDE_NUM2 > 7) & (agent_temp.AMPLITUDE_NUM2 < 12),  
      'AMPLI_GRANDE': (agent_temp.AMPLITUDE_NUM2 >= 12),  
    }
  }
  CFG_AMP_RATES = {
    "PART_SOIREE_HISTO":  {"Bcp_soirees": 0.3},
    "PART_HEUREDEBUT_TOT_HISTO": {"Bcp_debut_service_tot": 0.2},
    "PART_HEUREFIN_TRESTARD_HISTO": {"Bcp_fin_service_tard": 0.4},
    "PART_AMPLI_FAIBLE_HISTO": {"Bcp_faibles_amplitudes": 0.50},
    "PART_AMPLI_GRANDE_HISTO": {"Bcp_grandes_amplitudes": 0.50},
  }
elif filiale_name == 'autocarPlanche':
  CFG_AMPL_HOR = {
    "TRAME": {
      "SOIREE": (agent_temp.HEUREFIN_NUM2 <= 6),
      "JOURNEE": (agent_temp.HEUREFIN_NUM2 > 6),
    },
    "HDEB": { 
      'HEUREDEBUT_TOT': (agent_temp.HEUREDEBUT_NUM2 < 6),  
      'HEUREDEBUT_NORMAL': (agent_temp.HEUREDEBUT_NUM2 >= 6) & (agent_temp.HEUREDEBUT_NUM2 <= 9),
      'HEUREDEBUT_TARD': (agent_temp.HEUREDEBUT_NUM2 > 9)
    },
    "HFIN": {   
      'HEUREFIN_TOT': (agent_temp.HEUREFIN_NUM2 > 5) & (agent_temp.HEUREFIN_NUM2 <= 15),  
      'HEUREFIN_TARD': (agent_temp.HEUREFIN_NUM2 > 15) & (agent_temp.HEUREFIN_NUM2 <= 19),
      'HEUREFIN_TRESTARD': ((agent_temp.HEUREFIN_NUM2 > 19) | (agent_temp.HEUREFIN_NUM2 <= 5)),
    },
    "AMPL": {
      'AMPLI_FAIBLE': (agent_temp.AMPLITUDE_NUM2 < 5),  
      'AMPLI_MOYENNE': (agent_temp.AMPLITUDE_NUM2 >= 5) & (agent_temp.AMPLITUDE_NUM2 <= 11),  
      'AMPLI_GRANDE': (agent_temp.AMPLITUDE_NUM2 > 11),  
    }
  }
  CFG_AMP_RATES = {
    "PART_SOIREE_HISTO":  {"Bcp_soirees": 0.3},
    "PART_HEUREDEBUT_TOT_HISTO": {"Bcp_debut_service_tot": 0.3},
    "PART_HEUREFIN_TRESTARD_HISTO": {"Bcp_fin_service_tard": 0.45},
    "PART_AMPLI_FAIBLE_HISTO": {"Bcp_faibles_amplitudes": 0.2},
    "PART_AMPLI_GRANDE_HISTO": {"Bcp_grandes_amplitudes": 0.2},
  }
elif filiale_name == 'aerolis':
  CFG_AMPL_HOR = {
    "TRAME": {
      "SOIREE": (agent_temp.HEUREFIN_NUM2 <= 6) | (agent_temp.HEUREFIN_NUM2 > 22),
      "JOURNEE": ~((agent_temp.HEUREFIN_NUM2 <= 6) | (agent_temp.HEUREFIN_NUM2 > 22)),
    },
    "HDEB": { 
      'HEUREDEBUT_TOT': (agent_temp.HEUREDEBUT_NUM2 < 6),  
      'HEUREDEBUT_NORMAL': (agent_temp.HEUREDEBUT_NUM2 >= 6) & (agent_temp.HEUREDEBUT_NUM2 <= 9),
      'HEUREDEBUT_TARD': (agent_temp.HEUREDEBUT_NUM2 > 9)
    },
    "HFIN": {   
      'HEUREFIN_TOT': (agent_temp.HEUREFIN_NUM2 >= 4) & (agent_temp.HEUREFIN_NUM2 <= 20),  
      'HEUREFIN_TARD': (agent_temp.HEUREFIN_NUM2 > 20) & (agent_temp.HEUREFIN_NUM2 < 23),
      'HEUREFIN_TRESTARD': ((agent_temp.HEUREFIN_NUM2 >= 23) | (agent_temp.HEUREFIN_NUM2 < 4)),
    },
    "AMPL": {
      'AMPLI_FAIBLE': (agent_temp.AMPLITUDE_NUM2 <= 6),  
      'AMPLI_MOYENNE': (agent_temp.AMPLITUDE_NUM2 > 6) & (agent_temp.AMPLITUDE_NUM2 < 11),  
      'AMPLI_GRANDE': (agent_temp.AMPLITUDE_NUM2 >= 11),  
    }
  }
  CFG_AMP_RATES = {
    "PART_SOIREE_HISTO":  {"Bcp_soirees": 0.5},
    "PART_HEUREDEBUT_TOT_HISTO": {"Bcp_debut_service_tot": 0.5},
    "PART_HEUREFIN_TRESTARD_HISTO": {"Bcp_fin_service_tard": 0.5},
    "PART_AMPLI_FAIBLE_HISTO": {"Bcp_faibles_amplitudes": 0.01},
    "PART_AMPLI_GRANDE_HISTO": {"Bcp_grandes_amplitudes": 0.30},
  }

# SER_FREQ_ABS_2 = SER_FREQ_ABS.withColumn("dr_m3", F.unix_timestamp(F.add_months(F.col('date_roulement'), -3)))\
#                              .withColumn("dr", F.unix_timestamp(F.col("date_roulement")))
# SER_FREQ_ABS_2 = SER_FREQ_ABS_2.withColumn("datedelta", F.col("dr") - F.col("dr_m3"))
# # print(SER_FREQ_ABS_2.filter(F.col("datedelta").isNotNull()).approxQuantile("datedelta", [0.5, 0.9, 0.99, 0.999, 0.9999], 0)) # [3.0, 4.0, 5.0, 7.0, 14.0, 15.0, 16.0]
# # .hint("range_join", "7948800")
# agent_histo = SER_FREQ_ABS_2.join(agent_temp, (agent_temp.dr2 >= SER_FREQ_ABS_2.dr_m3) & 
#                                               (agent_temp.dr2 < SER_FREQ_ABS_2.dr) & 
#                                               (SER_FREQ_ABS_2.id_salarie == agent_temp.id_salarie2), how='left')\
#                             .groupBy(SER_FREQ_ABS_2.id_salarie.alias('id_salarie2'),
#                                      SER_FREQ_ABS_2.date_roulement.alias('date_roulement2'))\
#                             .agg(*[F.sum(F.when(NO_H_FILTER & cond, 1).otherwise(0)).alias(name) for _, dd in CFG_AMPL_HOR.items() for name, cond in dd.items()])
# #                             .filter(SER_FREQ_ABS_2.id_salarie == agent_temp.id_salarie2)\

agent_histo = SER_CALC_ABS.join(agent_temp, (SER_CALC_ABS.id_salarie == agent_temp.id_salarie2) &
                                            (agent_temp.date_roulement2 >= F.add_months(SER_CALC_ABS['date_roulement'], -3)) & 
                                            (agent_temp.date_roulement2 < SER_CALC_ABS.date_roulement), how='left')\
                          .groupBy(SER_CALC_ABS.id_salarie.alias('id_salarie2'),
                                   SER_CALC_ABS.date_roulement.alias('date_roulement2'))\
                          .agg(*[F.sum(F.when(NO_H_FILTER & cond, 1).otherwise(0)).alias(name) for _, dd in CFG_AMPL_HOR.items() for name, cond in dd.items()])


part_histo_list = []
to_drop = []
for cat, dd in CFG_AMPL_HOR.items():
  for name in dd:
    to_drop.append(name)
    part_name = "PART_" + name + "_HISTO"
    agent_histo = agent_histo.withColumn(part_name,  F.col(name) / sum( F.col(all_vars) for all_vars in dd))
    agent_histo = fill_nulls_2sides(agent_histo, part_name, "id_salarie2", "date_roulement2")
#     print(part_name, agent_histo.approxQuantile(part_name, [0.05, 0.25, 0.5, 0.75, 0.95], 0)) # [3.0, 4.0, 5.0, 7.0, 14.0, 15.0, 16.0]

for name, dd_rates in CFG_AMP_RATES.items():
    
    for new_name, val in dd_rates.items():
      agent_histo = agent_histo.withColumn(new_name, F.when(F.col(name) > dd_rates[new_name], 1).otherwise(0))
      part_histo_list += [new_name]

# Jointure avec les agents_roulements
ag_h = agent_histo.alias('ag_h')
ag_roul_id6 = SER_AMPL_TRAV.alias('ag_roul_id6')
SER_AMPL = ag_roul_id6.join(ag_h,(ag_roul_id6.date_roulement == ag_h.date_roulement2) &
                                     (ag_roul_id6.id_salarie == ag_h.id_salarie2), how='left')\
                                      .select('ag_roul_id6.*', *part_histo_list)
# export_result_df(SER_AMPL, "test.csv")
SER_AMPL = SER_AMPL.withColumn("Horaires", F.when(F.col("Bcp_debut_service_tot")==1, "Bcp_debut_service_tot").otherwise(
                                                  F.when(F.col("Bcp_fin_service_tard")==1, "Bcp_fin_service_tard").otherwise(
                                                  "intermediaire")))\
                   .withColumn("Amplitudes horaires", F.when(F.col("Bcp_faibles_amplitudes")==1, "Bcp_faibles_amplitudes").otherwise(
                                                      F.when(F.col("Bcp_grandes_amplitudes")==1, "Bcp_grandes_amplitudes").otherwise(
                                                      "intermediaire")))\
                   .withColumn("Service soiree", F.when(F.col("Bcp_soirees")==1, "Bcp_soirees").otherwise(
                                                      "Peu_soirees"))
SER_AMPL = SER_AMPL.drop("nb_h_service", "min_heure_debut_service", "max_heure_fin_service", "HEUREDEBUT_NUM", "HEUREFIN_NUM", "AMPLITUDE_NUM")

# SER_AMPL = SER_AMPL.fillna(0, "Bcp_faibles_amplitudes_hist")
# SER_AMPL = SER_AMPL.fillna(0, "Bcp_grandes_amplitudes_hist")
# SER_AMPL = bcache(SER_AMPL)



# COMMAND ----------

# DBTITLE 1,Calcul des variables sur l'objet service - Fréquence d'absence
SER_FREQ_ABS = SER_AMPL

#Ajout de la date d'entrée et de sortie du salarié au fichier de la variable absence ############################################
#Permet d'identifier, dans le calcul qui va suivre, si le salarié est en absence ou est sortie de l'entreprise

VARIABLE_ABS = SER_FREQ_ABS.alias('VARIABLE_ABS')

contratInte = read_csv("contrat.csv")
CONTRAT_FOR_ABS = contratInte.select("mois_annee", "id_salarie", "date_debut_contrat", "date_fin_contrat", "nature_contrat")
CONTRAT_TEMP = CONTRAT_FOR_ABS.select(F.col("mois_annee").alias('mois_annee2'), 
                               F.col("id_salarie").alias("id_salarie2"), 
                               F.col("date_debut_contrat").alias('date_debut_contrat2'), 
                               F.col("date_fin_contrat").alias('date_fin_contrat2')) 
#Jointure entre les objets fonction et contrat via l'id du salarie et le mois et l'annee de la donnée
VARIABLE_ABS = VARIABLE_ABS.join(CONTRAT_TEMP, ((F.trim(F.col('id_salarie')) == F.trim(F.col('id_salarie2'))) & (F.trim(F.col('mois_annee')) == F.trim(F.col('mois_annee2')))), how='left')\
                            .select(F.col('mois_annee2'), F.col('date_debut_contrat2'), F.col('date_fin_contrat2'), 'VARIABLE_ABS.*')

VARIABLE_ABS = VARIABLE_ABS.withColumn("FLAG_ENTREE", F.when(F.col('date_debut_contrat2') == F.col('date_roulement'), 1).otherwise(0))\
                           .withColumn("FLAG_SORTIE", F.when(F.col('date_fin_contrat2') == F.col('date_roulement'), 1).otherwise(0))

VARIABLE_ABS = VARIABLE_ABS.fillna(0, subset=['FLAG_ENTREE', 'FLAG_SORTIE'])

VARIABLE_ABS = VARIABLE_ABS.drop('mois_annee2')\
                            .drop('date_debut_contrat2')\
                            .drop('date_fin_contrat2')

###################################################################################################################################

#Calcul du temps d'entrée et de sortie 3 mois autour d'une absence ################################################################

VARIABLE_ABS_TEMP1 = VARIABLE_ABS.select("id_salarie", "date_roulement", "ID_SERVICE_ABSENCE", "FLAG_ABS", "FLAG_ENTREE", "FLAG_SORTIE").distinct()

VARIABLE_ABS_TEMP = VARIABLE_ABS_TEMP1.select(F.col("id_salarie").alias('id_salarie2'), 
                               F.col("date_roulement").alias("date_roulement2"), 
                               F.col("FLAG_ENTREE").alias('FLAG_ENTREE2'), 
                               F.col("FLAG_SORTIE").alias('FLAG_SORTIE2'), 
                               F.col("ID_SERVICE_ABSENCE").alias('ID_SERVICE_ABSENCE2'),
                               F.col("FLAG_ABS").alias('FLAG_ABS2'))

#Chercher si entrée dans les 3 mois précédents une absence
VARIABLE_ABS_TEMP_E = VARIABLE_ABS_TEMP1.join(VARIABLE_ABS_TEMP, VARIABLE_ABS_TEMP1.id_salarie == VARIABLE_ABS_TEMP.id_salarie2, how='left')\
                                        .filter((VARIABLE_ABS_TEMP.date_roulement2 > F.add_months(VARIABLE_ABS_TEMP1.date_roulement, -3)) & 
                                              (VARIABLE_ABS_TEMP.date_roulement2 <= VARIABLE_ABS_TEMP1.date_roulement))\
                                        .groupBy(VARIABLE_ABS_TEMP1.id_salarie.alias('id_salarie2'), VARIABLE_ABS_TEMP1.date_roulement.alias('date_roulement2'))\
                                        .agg(F.sum(VARIABLE_ABS_TEMP.FLAG_ENTREE2).alias('ENTREE_HISTO'))

ag_histo = VARIABLE_ABS_TEMP_E.alias('ag_histo')
ag_roul_id5 = SER_FREQ_ABS.alias('ag_roul_id5')
SER_FREQ_ABS = ag_roul_id5.join(ag_histo, (ag_roul_id5.date_roulement == ag_histo.date_roulement2) & 
                                          (ag_roul_id5.id_salarie == ag_histo.id_salarie2), how='left')\
                          .select('ag_roul_id5.*', 'ag_histo.ENTREE_HISTO')

#Chercher si sortie dans les 3 mois suivant une absence
VARIABLE_ABS_TEMP_S = VARIABLE_ABS_TEMP1.join(VARIABLE_ABS_TEMP, VARIABLE_ABS_TEMP1.id_salarie == VARIABLE_ABS_TEMP.id_salarie2, how='left')\
                                        .filter((VARIABLE_ABS_TEMP.date_roulement2 < F.add_months(VARIABLE_ABS_TEMP1['date_roulement'], 3)) & 
                                                (VARIABLE_ABS_TEMP.date_roulement2 >= VARIABLE_ABS_TEMP1.date_roulement))\
                                        .groupBy(VARIABLE_ABS_TEMP1.id_salarie.alias('id_salarie2'), VARIABLE_ABS_TEMP1.date_roulement.alias('date_roulement2'))\
                                        .agg(F.sum(VARIABLE_ABS_TEMP.FLAG_SORTIE2).alias('SORTIE_HISTO'))

ag_histo = VARIABLE_ABS_TEMP_S.alias('ag_histo')
ag_roul_id5 = SER_FREQ_ABS.alias('ag_roul_id5')
SER_FREQ_ABS = ag_roul_id5.join(ag_histo, (ag_roul_id5.date_roulement == ag_histo.date_roulement2) & 
                                          (ag_roul_id5.id_salarie == ag_histo.id_salarie2), how='left')\
                          .select('ag_roul_id5.*', 'ag_histo.SORTIE_HISTO')

SER_FREQ_ABS = SER_FREQ_ABS.fillna(0, subset=['ENTREE_HISTO', 'SORTIE_HISTO'])

###################################################################################################################################

# Jointure avec les agents_roulements #############################################################################################

agent_histo = SER_FREQ_ABS.join(VARIABLE_ABS_TEMP, SER_FREQ_ABS.id_salarie == VARIABLE_ABS_TEMP.id_salarie2, how='left')\
                                .filter((VARIABLE_ABS_TEMP.date_roulement2 > F.add_months(SER_FREQ_ABS['date_roulement'], -3)) & 
                                        (VARIABLE_ABS_TEMP.date_roulement2 <= SER_FREQ_ABS.date_roulement) & 
                                        (VARIABLE_ABS_TEMP.FLAG_ABS2==1) )\
                                .groupBy(SER_FREQ_ABS.id_salarie,
                                         SER_FREQ_ABS.date_roulement,
                                         SER_FREQ_ABS.ID_SERVICE_ABSENCE)\
                                .agg(F.countDistinct(VARIABLE_ABS_TEMP.ID_SERVICE_ABSENCE2).alias('FREQUENCE_ABS_HISTO'),                                                                               
                                     F.count(VARIABLE_ABS_TEMP.ID_SERVICE_ABSENCE2).alias('DUREE_ABS_HISTO'))\
                                .withColumn('FREQUENCE_ABS_HISTO', F.when(F.col("FREQUENCE_ABS_HISTO").cast("integer").isNull(),0)\
                                                                    .otherwise(F.col("FREQUENCE_ABS_HISTO")))\
                                .withColumn('DUREE_ABS_HISTO', F.when(F.col("DUREE_ABS_HISTO").cast("integer").isNull(),0)\
                                                                .otherwise(F.col("DUREE_ABS_HISTO")))

agent_histo = agent_histo.select(F.col("id_salarie").alias('id_salarie2'),
                                 F.col("date_roulement").alias("date_roulement2"),
                                 F.col("DUREE_ABS_HISTO"), F.col("FREQUENCE_ABS_HISTO")).distinct()

# display(agent_histo.filter((F.col("date_roulement2")> F.lit('2019-04-01'))).select("id_salarie2", "date_roulement2", "DUREE_ABS_HISTO", "FREQUENCE_ABS_HISTO"))

ag_histo = agent_histo.alias('ag_histo')
ag_roul_id5 = SER_FREQ_ABS.alias('ag_roul_id5')
ag_roul_id5 = ag_roul_id5.drop('FREQUENCE_ABS_HISTO')\
                          .drop('DUREE_ABS_HISTO')

SER_FREQ_ABS = ag_roul_id5.join(ag_histo,(ag_roul_id5.date_roulement==ag_histo.date_roulement2) &
                                         (ag_roul_id5.id_salarie==ag_histo.id_salarie2), how='left')\
                          .select('ag_roul_id5.*','ag_histo.FREQUENCE_ABS_HISTO','ag_histo.DUREE_ABS_HISTO' ).distinct()\
                          .withColumn('FREQUENCE_ABS_HISTO', F.when(F.col("FREQUENCE_ABS_HISTO").cast("integer").isNull(),0)\
                                                              .otherwise(F.col("FREQUENCE_ABS_HISTO")))\
                          .withColumn('DUREE_ABS_HISTO', F.when(F.col("DUREE_ABS_HISTO").cast("integer").isNull(),0)\
                                                          .otherwise(F.col("DUREE_ABS_HISTO")))
# display(SER_FREQ_ABS.filter((F.col("date_roulement")> F.lit('2019-02-01')) & (F.col("FLAG_ABS")==1)).select("id_salarie", "date_roulement", "FLAG_ABS", "FREQUENCE_ABS_HISTO", "DUREE_ABS_HISTO"))


# Création des variables FREQUENCE_ABS_HISTO_MOIS & DUREE_ABS_HISTO_MOIS ###########################################################

my_window = Window.partitionBy("id_salarie","mois_annee").orderBy("id_salarie","mois_annee","date_roulement").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
SER_FREQ_ABS = SER_FREQ_ABS\
                        .withColumn("FREQUENCE_ABS_HISTO_MOIS", F.last(F.col("FREQUENCE_ABS_HISTO")).over(my_window))\
                        .withColumn("DUREE_ABS_HISTO_MOIS", F.last(F.col("DUREE_ABS_HISTO")).over(my_window))

SER_FREQ_ABS = SER_FREQ_ABS\
          .withColumn("FREQUENCE_ABS_HISTO_MOIS", F.when(F.col("FREQUENCE_ABS_HISTO_MOIS").isNull(),0)\
                                                   .otherwise(F.col("FREQUENCE_ABS_HISTO_MOIS")))\
          .withColumn("DUREE_ABS_HISTO_MOIS", F.when(F.col("DUREE_ABS_HISTO_MOIS").isNull(),0)\
                                               .otherwise(F.col("DUREE_ABS_HISTO_MOIS")))
####################################################################################################################################

#Calcul des fréquences d'absence en fonction de FREQUENCE_ABS_HISTO_MOIS et DUREE_ABS_HISTO_MOIS ###################################

SER_FREQ_ABS = SER_FREQ_ABS.withColumn("TYPO_ABS_HISTO",
                                F.when(F.col("ENTREE_HISTO")>0,"00_ENTREE")\
                                .otherwise(F.when(F.col("SORTIE_HISTO")>0,"00_SORTIE")\
                                .otherwise(F.when(F.col("FREQUENCE_ABS_HISTO_MOIS")==0,"0_SS_ABS")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")==1) & (F.col("DUREE_ABS_HISTO_MOIS")==1),"1_1JOUR_ABSENCE")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")==1) & (F.col("DUREE_ABS_HISTO_MOIS")>=89),"6_3MOIS_ABSENCE")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")<=2) & (F.col("DUREE_ABS_HISTO_MOIS")<7),"2_<=3ABS_<1SEM")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")<=2) & (F.col("DUREE_ABS_HISTO_MOIS")<32),"3_<=3ABS_<1MOIS")\
                                .otherwise(F.when(F.col("FREQUENCE_ABS_HISTO_MOIS")<=2 ,"4_<=3ABS_>=1MOIS")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")<=3) & (F.col("DUREE_ABS_HISTO_MOIS")<7),"2_<=3ABS_<1SEM")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")<=3) & (F.col("DUREE_ABS_HISTO_MOIS")<32) ,"3_<=3ABS_<1MOIS")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")<=3) ,"4_<=3ABS_>=1MOIS")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")>3) & (F.col("DUREE_ABS_HISTO_MOIS")<32) ,"5_>3ABS")\
                                .otherwise(F.when((F.col("FREQUENCE_ABS_HISTO_MOIS")>3) ,"5_>3ABS")))))))))))) ))
SER_FREQ_ABS = SER_FREQ_ABS.withColumn("ENTREE_OU_SORTIE", F.when((F.col("ENTREE_HISTO") > 0) | (F.col("SORTIE_HISTO") > 0), 1).otherwise(0))
                                       
SER_FREQ_ABS = SER_FREQ_ABS.drop("FLAG_ENTREE", "FLAG_SORTIE", "ENTREE_HISTO", "SORTIE_HISTO")

####################################################################################################################################




# COMMAND ----------

eSER_FREQ_ABS = cache(SER_FREQ_ABS, "SER_FREQ_ABS.csv", False)

# COMMAND ----------

# DBTITLE 1,Calcul des variables sur les objets contrat et salarie
#Merge des objets contrat et salarié avec le fichier de calcul des variables sur l'objet service #################################################
CONT = CON.withColumnRenamed("id_salarie", "id_salarie2")
CON2 = CONT.alias("CON2")
SAL2 = SAL.alias("SAL2")


CS_VAR = CON2.join(SAL2, (F.trim(SAL2.id_salarie) == F.trim(CON2.id_salarie2)), how='left')\
              .select('CON2.*','SAL2.*')



CS_VAR = CS_VAR.withColumnRenamed("mois_annee","mois_annee2")
CS_VAR = CS_VAR.drop("id_salarie")

SER_FREQ_ABS
#Jointure entre les fichiers
SER_VAR = SER_FREQ_ABS.alias('SER_VAR').filter(F.col("date_roulement")>F.lit("2016-01-01"))
CS_VAR = CS_VAR.alias('CS_VAR')

CS_VAR_SER_VAR = SER_VAR.join(CS_VAR, (F.trim(CS_VAR.id_salarie2) == F.trim(SER_VAR.id_salarie)) & (CS_VAR.mois_annee2 == SER_VAR.mois_annee), how='left')\
                       .select('SER_VAR.*', 'CS_VAR.*')


SER_VAR2 = CS_VAR_SER_VAR.alias('SER_VAR2')
FON2 = FON.alias('FON2')

CS_VAR_SER_VAR = SER_VAR2.join(FON2, (FON2.id_salarie == SER_VAR2.id_salarie) & (FON2.mois_annee == SER_VAR2.mois_annee), how='left')\
                       .select('SER_VAR2.*', 'FON2.lib_activite', "FON2.emploi_occupe")

CS_VAR_SER_VAR = CS_VAR_SER_VAR.drop('id_salarie2')\
                               .drop('mois_annee2')
###############################################################################################################################################
#Calcul des variables liées au contrat ########################################################################################################

CON_VAR = CS_VAR_SER_VAR
CON_VAR = CON_VAR.withColumn("Temps_partiel", F.when(F.col("temps_travail_pourcent")<100, 1).otherwise(0))
if filiale_name == "aerolis":
  CON_VAR = CON_VAR.withColumn("Temps_partiel", F.when(F.col("temps_travail_mensuel")<148.2, 1).otherwise(0))


CON_VAR = CON_VAR.withColumn("Anciennete_entree", (F.datediff(F.col('date_roulement'),F.col('date_embauche_societe'))/365).cast('integer'))\
                 .withColumn('Anciennete_moins_1an', F.when((F.col("Anciennete_entree")< 1),1).otherwise(0))\
                 .withColumn('Anciennete_1an_5ans', F.when((F.col("Anciennete_entree")>= 1) & (F.col("Anciennete_entree")<= 5) ,1).otherwise(0))\
                 .withColumn('Anciennete_6ans_10ans', F.when((F.col("Anciennete_entree")> 5) & (F.col("Anciennete_entree")<= 10) ,1).otherwise(0))\
                 .withColumn('Anciennete_11ans_20ans', F.when((F.col("Anciennete_entree")> 10) & (F.col("Anciennete_entree")<= 20) ,1).otherwise(0))\
                 .withColumn('Anciennete_21ans_30ans', F.when((F.col("Anciennete_entree")> 20) & (F.col("Anciennete_entree")<= 30) ,1).otherwise(0))\
                 .withColumn('Anciennete_plus_30ans', F.when((F.col("Anciennete_entree")> 30),1).otherwise(0))\
                 .withColumn('Stage', F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_STA), 1).otherwise(0))\
                 .withColumn('CDI', F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CDI), 1).otherwise(0))\
                 .withColumn('CDD_vacation', F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_VAC), 1).otherwise(0))\
                 .withColumn('CDD', F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CDD), 1).otherwise(0))\
                 .withColumn('CPS', F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CPS), 1).otherwise(0))\
                 .withColumn("Contrat", F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CDI), "CDI")\
                              .otherwise(F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CDD), "CDD")\
                              .otherwise(F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_STA), "Stage")\
                              .otherwise(F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_CPS), "CPS")\
                              .otherwise(F.when(F.trim(F.col("nature_contrat")).isin(CON_NC_VAC), "Vacation")\
                              .otherwise("donnee manquante"))))))\
                 .withColumn('Anciennete', F.when(F.col("Anciennete_moins_1an")==1, "moins_1an").otherwise(
                                                   F.when(F.col("Anciennete_1an_5ans")==1, "1an_5ans").otherwise(  
                                                   F.when(F.col("Anciennete_6ans_10ans")==1, "6ans_10ans").otherwise(  
                                                   F.when(F.col("Anciennete_11ans_20ans")==1, "11ans_20ans").otherwise(  
                                                   F.when(F.col("Anciennete_21ans_30ans")==1, "21ans_30ans").otherwise(  
                                                   F.when(F.col("Anciennete_plus_30ans")==1, "plus_30ans").otherwise("donnee manquante")))))))  
###############################################################################################################################################.filter(F.col("date_roulement") > F.lit("01/01/2019"))

# Gestion des variables Salarié ###############################################################################################################                            
                             
SAL_VAR = CON_VAR
# display(SAL_VAR.orderBy("date_roulement", "id_salarie"))

SAL_VAR = SAL_VAR.withColumn("Homme", F.when(F.trim(F.col("sexe")).isin(SAL_SE_H), 1).otherwise(0))\
                 .withColumn("Femme", F.when(F.trim(F.col("sexe")).isin(SAL_SE_F), 1).otherwise(0))\
                 .withColumn("Genre_non_fourni", F.when((F.col("Homme")==0) & (F.col("Femme")==0), 1).otherwise(0))\
                 .withColumn("Sexe", F.when(F.trim(F.col("sexe")).isin(SAL_SE_H), "Homme").otherwise(
                                            F.when(F.trim(F.col("sexe")).isin(SAL_SE_F), "Femme").otherwise("Genre_non_fourni")))\
                 .withColumn("En_couple", F.when(F.trim(F.col("situation_familiale")).isin(SAL_SF),1).otherwise(0))\
                 .withColumn("Celibataire", F.when(F.trim(F.col("situation_familiale")).isin(SAL_SF), 0).otherwise(1))\
                  .withColumn("Situation_familiale", F.when(F.col("En_couple")==1, "En_couple").otherwise("Celibataire"))

# display(SAL_VAR.groupBy("situation_familiale").count())
# display(SAL_VAR.orderBy("date_roulement", "id_salarie"))

###############################################################################################################################################
                               
# Gestion de l'age des enfants (Transpole) #################################################################################################### 
SER_VAR_CHILDS = SAL_VAR

# display(SER_VAR_CHILDS.select("id_salarie", "date_roulement", "date_naissance_enfant_1", "date_naissance_enfant_2", "date_naissance_enfant_3", "date_naissance_enfant_4"))

SER_VAR_CHILDS = SER_VAR_CHILDS.withColumn("Nb_enfant<=20ans", ( F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_1").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_2").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_3").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_4").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_5").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_6").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_7").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_8").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_9").cast('integer'))<=20),1).otherwise(0) + F.when(((F.year("date_roulement").cast('integer')-F.year("date_naissance_enfant_10").cast('integer'))<=20),1).otherwise(0)))\
                                .withColumn('0_enfant_moins_20ans', F.when(F.col("Nb_enfant<=20ans")==0 ,1).otherwise(0))\
                                .withColumn('1_enfant_moins_20ans', F.when(F.col("Nb_enfant<=20ans")==1 ,1).otherwise(0))\
                                .withColumn('2plus_enfants_moins_20ans', F.when(F.col("Nb_enfant<=20ans")>1 ,1).otherwise(0))\
                                .withColumn('Enfants', F.when(F.col("0_enfant_moins_20ans")==1, "0_enfant_moins_20ans").otherwise(
                                                       F.when(F.col("1_enfant_moins_20ans")==1, "1_enfant_moins_20ans").otherwise(  
                                                       F.when(F.col("2plus_enfants_moins_20ans")==1, "2plus_enfants_moins_20ans").otherwise(""))))  


  
###############################################################################################################################################                              
                               
# Gestion de l'age ############################################################################################################################ 

SER_VAR_AGE = SER_VAR_CHILDS

SER_VAR_AGE = SER_VAR_AGE.withColumn("Age", F.year("date_roulement").cast('integer')-F.year(F.col("date_naissance")).cast('integer'))\
                         .withColumn('Age_moins_26', F.when(F.col("Age")< 26 , 1).otherwise(0))\
                         .withColumn('Age_26_30', F.when((F.col("Age")>=26) & (F.col("Age") <= 30), 1).otherwise(0))\
                         .withColumn('Age_31_40', F.when((F.col("Age")>=31) & (F.col("Age") <= 40), 1).otherwise(0))\
                         .withColumn('Age_41_50', F.when((F.col("Age")>=41) & (F.col("Age") <= 50), 1).otherwise(0))\
                         .withColumn('Age_51_60', F.when((F.col("Age")>=51) & (F.col("Age") <= 60), 1).otherwise(0))\
                         .withColumn('Age_plus_60', F.when((F.col("Age")> 60), 1).otherwise(0))\
                         .withColumn('Tranche_Age', F.when(F.col("Age_moins_26")==1, "moins_26").otherwise(
                                                   F.when(F.col("Age_26_30")==1, "26_30").otherwise(  
                                                   F.when(F.col("Age_31_40")==1, "31_40").otherwise(  
                                                   F.when(F.col("Age_41_50")==1, "41_50").otherwise(  
                                                   F.when(F.col("Age_51_60")==1, "51_60").otherwise(  
                                                   F.when(F.col("Age_plus_60")==1, "plus_60").otherwise("donnee manquante")))))))  

# display(SER_VAR_AGE.orderBy("date_roulement", "id_salarie"))
if (filiale_name == "besancon"):
  horaire_mensuel = REM.approxQuantile("_400_B_Hr_Contrat", [0.5], 0)[0]
else:
  horaire_mensuel = CON.approxQuantile("temps_travail_mensuel", [0.5], 0)[0]

print(horaire_mensuel)
SER_VAR_AGE = fill_nulls_2sides(SER_VAR_AGE, "temps_travail_mensuel")
SER_VAR_AGE = SER_VAR_AGE.withColumn("temps_travail_mensuel", F.when(F.col("temps_travail_mensuel").isNull(), horaire_mensuel).otherwise(F.col("temps_travail_mensuel")))
SER_VAR_AGE = SER_VAR_AGE.withColumn('heures_service', F.when((F.col("heures_service")==0), F.col("temps_travail_mensuel") / 26).otherwise(F.col("heures_service")))
# & (F.col("FLAG_ABS_RAW")==1)
SER_VAR_AGE = SER_VAR_AGE.withColumn('heures_service', F.col("temps_travail_mensuel") / 30)


# tt = SER_VAR_AGE#.filter((F.col("date_roulement")==F.lit("2017-06-18"))|(F.col("date_roulement")==F.lit("2017-06-19")) |(F.col("date_roulement")==F.lit("2017-06-20")))
# tt = tt.withColumn("month", F.month(F.col("date_roulement"))).withColumn("year", F.year(F.col("date_roulement")))
# tt = tt.orderBy("id_salarie", "date_roulement").select("id_salarie", "date_roulement", "year", "month", "FLAG_ABS", "heure_debut_service", "heure_fin_service", "temps_travail_mensuel", "heures_service")
# tt = tt.withColumn("ABS", F.when(F.col("FLAG_ABS")==1, F.col("heures_service") ).otherwise(0))
# tt = tt.withColumn("PRE", F.when(F.col("FLAG_ABS")==0, F.col("heures_service") ).otherwise(0))
# tt2 = tt.filter(F.col("date_roulement")>F.lit("2018-01-01")).groupBy("year", "month").agg(F.sum("ABS").alias("ABS"), F.sum("PRE").alias("PRE"), F.sum("FLAG_ABS").alias("ABS_D"), F.count("id_salarie").alias("TOT"))
# tt2 = tt2.withColumn("tx", F.col("ABS")/(F.col("ABS")+F.col("PRE")))
# tt2 = tt2.withColumn("td", F.col("ABS_D")/(F.col("TOT")))

# display(tt2.orderBy("year", "month"))

# tt2 = tt.filter(F.col("date_roulement")>F.lit("2018-01-01")).groupBy("date_roulement").agg(F.sum("ABS").alias("ABS"), F.sum("PRE").alias("PRE"), F.sum("FLAG_ABS").alias("ABS_D"), F.count("id_salarie").alias("TOT"))
# tt2 = tt2.withColumn("tx", F.col("ABS")/(F.col("ABS")+F.col("PRE")))
# tt2 = tt2.withColumn("td", F.col("ABS_D")/(F.col("TOT")))
# display(tt2.orderBy("date_roulement"))
SER_VAR_AGE = SER_VAR_AGE.withColumn("lib_categorie_professionnelle", F.when(F.col("lib_categorie_professionnelle").isNull(), F.col("lib_activite") ).otherwise(F.col("lib_categorie_professionnelle")))
SER_VAR_AGE = SER_VAR_AGE.withColumn("lib_categorie_professionnelle", F.when(F.col("lib_categorie_professionnelle").isNull(), F.col("emploi_occupe") ).otherwise(F.col("lib_categorie_professionnelle")))
SER_VAR_AGE = SER_VAR_AGE.withColumn("lib_categorie_professionnelle", F.when(F.col("lib_categorie_professionnelle").isNull(), "donnee manquante" ).otherwise(F.col("lib_categorie_professionnelle")))

CON_CP = get_col_unique(SER_VAR_AGE, "lib_categorie_professionnelle")

SER_VAR_AGE = SER_VAR_AGE.drop("date_embauche_societe", "motif_depart_societe", "id_categorie_professionnelle", "temps_travail_pourcent", "temps_travail_mensuel", "temps_mal_mensuel", "temps_mat_mensuel", "temps_mtt_mensuel", "temps_at_mensuel", "temps_attraj", "date_debut_contrat", "date_fin_contrat", "date_fin_periode_essai", "nature_contrat", "date_depart_societe", "lieu_travail_different", "adresse_lieu_travail", "code_postal_lieu_travail", "commune_lieu_travail", "filiale", "code_postal", "ville", "pays", "nb_permis", "date_naissance", "date_naissance_enfant_1", "date_naissance_enfant_2", "date_naissance_enfant_3", "date_naissance_enfant_4", "date_naissance_enfant_5", "date_naissance_enfant_6", "date_naissance_enfant_7", "date_naissance_enfant_8", "date_naissance_enfant_9", "date_naissance_enfant_10")
###############################################################################################################################################                 
# display(SER_VAR_AGE)

# COMMAND ----------

# DBTITLE 1,Calcul des variables sur les objets remuneration
SALAIRE_STAT = REM

### valeurs de champs variables par filiales
STATIC_REM_PERC = {}  
if filiale_name == 'transpole':
  STATIC_REM_PERC = {"low": 0.3, "med": 0.5, "high": 0.7}
elif filiale_name == 'besancon':
  STATIC_REM_PERC = {"low": 0.3, "med": 0.5, "high": 0.7}
elif filiale_name == 'autocarPlanche':
  STATIC_REM_PERC = {"low": 0.2, "med": 0.5, "high": 0.8}
elif filiale_name == 'aerolis':
  STATIC_REM_PERC = {"low": 0.2, "med": 0.5, "high": 0.8}
  
# display(CON)
# display(SALAIRE_STAT.select("BRUT_MENSUEL", "mois_annee"))

if (filiale_name == "besancon"):
  horaire_mensuel = SALAIRE_STAT.approxQuantile("_400_B_Hr_Contrat", [0.5], 0)[0]
else:
  df1 = SALAIRE_STAT.alias('df1')
  df2 = CON.alias('df2')
  SALAIRE_STAT = df1.join(df2, ((df1.mois_annee == df2.mois_annee) & (df1.id_salarie == df2.id_salarie)), how='left')\
                    .select("df1.mois_annee", "df1.id_salarie", "df1.BRUT_MENSUEL", "df2.temps_travail_mensuel")
  
  horaire_mensuel = SALAIRE_STAT.approxQuantile("temps_travail_mensuel", [0.5], 0)[0]
  SALAIRE_STAT = SALAIRE_STAT.withColumn("temps_travail_mensuel", F.when(F.col("temps_travail_mensuel").isNull(), horaire_mensuel).otherwise(F.col("temps_travail_mensuel")))
  if (filiale_name == "autocarPlanche") |  (filiale_name == "aerolis"):
    SALAIRE_STAT = SALAIRE_STAT.withColumn("_304_tx_hor", F.col("BRUT_MENSUEL") / F.col("temps_travail_mensuel"))
  else:
    SALAIRE_STAT = SALAIRE_STAT.withColumn("_304_tx_hor", F.col("BRUT_MENSUEL") / horaire_mensuel)


SALAIRE_STAT = SALAIRE_STAT.withColumn("SALAIRE_EQ_TP", F.col("_304_tx_hor") * horaire_mensuel)
# display(SALAIRE_STAT.groupBy("SALAIRE_EQ_TP").count().orderBy("SALAIRE_EQ_TP"))

plow = F.expr('percentile_approx(SALAIRE_EQ_TP, {})'.format(STATIC_REM_PERC["low"]))
pmed = F.expr('percentile_approx(SALAIRE_EQ_TP, {})'.format(STATIC_REM_PERC["med"]))
phigh = F.expr('percentile_approx(SALAIRE_EQ_TP, {})'.format(STATIC_REM_PERC["high"]))

salaires_q = SALAIRE_STAT.groupBy('mois_annee').agg(plow.alias('salaire_plow'), pmed.alias('salaire_pmed'), phigh.alias('salaire_phigh') ) 

if filiale_name == "aerolis":
  salaires_q = salaires_q.withColumn("salaire_plow", F.lit(2100.0))\
                        .withColumn("salaire_pmed", F.lit(2200.0))\
                        .withColumn("salaire_phigh", F.lit(2800.0))\
# display(salaires_q)
df1 = SALAIRE_STAT.alias("df1")
df2 = salaires_q.alias("df2")
SALAIRE_STAT = df1.join(df2, 
                    df1.mois_annee==df2.mois_annee, how='left')\
                    .select("df1.*", "df2.salaire_plow", "df2.salaire_pmed", "df2.salaire_phigh")

# Salaires = Salaires.withColumn("salaire_bas", F.when(F.col("SALAIRE_EQ_TP") <= F.col("salaire_plow"), 1).otherwise(0))\
#                     .withColumn("salaire_eleve", F.when(F.col("SALAIRE_EQ_TP") >= F.col("salaire_phigh"), 1).otherwise(0))






# quant = SALAIRE_STAT.approxQuantile("SALAIRE_EQ_TP", [STATIC_REM_PERC["low"], STATIC_REM_PERC["med"], STATIC_REM_PERC["high"]], 0) # [3.0, 4.0, 5.0, 7.0, 14.0, 15.0, 16.0]

# plow = quant[0] #F.expr('percentile_approx(BRUT_MENSUEL, {})'.format(STATIC_REM_PERC["low"]))
# pmed = quant[1] #F.expr('percentile_approx(BRUT_MENSUEL, {})'.format(STATIC_REM_PERC["med"]))
# phigh = quant[2] #F.expr('percentile_approx(BRUT_MENSUEL, {})'.format(STATIC_REM_PERC["high"]))

# print(horaire_mensuel)
# print(plow, pmed, phigh)
# if filiale_name == "aerolis":
#     REM_CAL = SALAIRE_STAT.withColumn("Salaire", F.lit("donnee non pertinente"))\
#                           .withColumn("salaire_bas", F.lit(0))\
#                           .withColumn("salaire_moyen", F.lit(0))\
#                           .withColumn("salaire_eleve", F.lit(0))
# else:
REM_CAL = SALAIRE_STAT.withColumn("salaire_bas", F.when(F.col("SALAIRE_EQ_TP") <= F.col("salaire_plow"), 1).otherwise(0))\
                   .withColumn("salaire_moyen", F.when((F.col("SALAIRE_EQ_TP") > F.col("salaire_plow")) & (F.col("SALAIRE_EQ_TP") < F.col("salaire_phigh")), 1).otherwise(0))\
                   .withColumn("salaire_eleve", F.when(F.col("SALAIRE_EQ_TP") >= F.col("salaire_phigh"), 1).otherwise(0))\
                   .withColumn("Salaire", F.when(F.col("salaire_bas")==1, "bas").otherwise(
                                          F.when(F.col("salaire_moyen")==1, "moyen").otherwise(
                                          F.when(F.col("salaire_eleve")==1, "eleve").otherwise("intermediaire"))))


# REM_CAL = SALAIRE_STAT.withColumn("salaire_bas", F.when(F.col("SALAIRE_EQ_TP") <= F.col("salaire_plow"), 1).otherwise(0))\
#                        .withColumn("salaire_moyen", F.when((F.col("SALAIRE_EQ_TP") > F.col("salaire_plow")) & (F.col("SALAIRE_EQ_TP") < F.col("salaire_phigh")), 1).otherwise(0))\
#                        .withColumn("salaire_eleve", F.when(F.col("SALAIRE_EQ_TP") >= F.col("salaire_phigh"), 1).otherwise(0))\
#                        .withColumn("Salaire", F.when(F.col("salaire_bas")==1, "bas").otherwise(
#                                               F.when(F.col("salaire_moyen")==1, "moyen").otherwise(
#                                               F.when(F.col("salaire_eleve")==1, "eleve").otherwise("intermediaire"))))


# REM_CAL.groupby("Salaire").count()
# display(REM_CAL.groupby("Salaire").count())
windowRemuneration = Window.partitionBy("id_salarie").orderBy("mois_annee")
  
REM_CAL = REM_CAL.withColumn("mois_lag", F.lag("mois_annee").over(windowRemuneration))
REM_CAL = REM_CAL.withColumn("salaire_lag", F.lag("SALAIRE_EQ_TP").over(windowRemuneration))
REM_CAL = fill_nulls(REM_CAL, "salaire_lag", "id_salarie", "mois_annee", False)

REM_CAL = REM_CAL.withColumn("DIFF_DATE", F.datediff(REM_CAL.mois_annee, REM_CAL.mois_lag)).fillna(30, "DIFF_DATE")

REM_CAL = REM_CAL.withColumn("ID_SALAIRE", F.sum( F.when((F.col("SALAIRE_EQ_TP") == F.col("salaire_lag")),0)\
                                           .otherwise(1)).over(windowRemuneration))

windowRemuneration = Window.partitionBy("id_salarie", "ID_SALAIRE").orderBy("mois_annee")
  
REM_CAL = REM_CAL.withColumn("duree_periode_salaire", F.sum("DIFF_DATE").over(windowRemuneration))\
         .drop("salaire_plow", "salaire_pmed", "salaire_phigh", "mois_lag", "DIFF_DATE", "ID_SALAIRE")
print(REM_CAL.approxQuantile("duree_periode_salaire", [0.05, 0.25, 0.5, 0.75, 0.95], 0)) # [3.0, 4.0, 5.0, 7.0, 14.0, 15.0, 16.0]

REM_CAL = REM_CAL.withColumn("augmentation_recente", F.when(F.col("duree_periode_salaire") < 190, 1).otherwise(0))\
                 .withColumn("augmentation_ancienne", F.when(F.col("duree_periode_salaire") >= 890, 1).otherwise(0))\
                 .withColumn("Augmentation", F.when(F.col("augmentation_recente")==1, "recente").otherwise(
                                             F.when(F.col("augmentation_ancienne")==1, "ancienne").otherwise("intermediaire")))\
                 .drop("duree_periode_salaire", "salaire_lag", "salaire_plow", "salaire_pmed", "salaire_phigh")

REM_CAL = REM_CAL.fillna("donnee manquante", subset=["Augmentation"])

REM_CAL = REM_CAL.withColumnRenamed("id_salarie", "id_salarie2")\
                 .withColumnRenamed("mois_annee", "mois_annee2")



VAR = SER_VAR_AGE.alias('VAR')
VAR_REM = REM_CAL.alias('VAR_REM')

VAR_REM = VAR.join(VAR_REM, ((VAR.mois_annee == VAR_REM.mois_annee2) & (VAR.id_salarie == VAR_REM.id_salarie2)), how='left')\
             .drop("id_salarie2", "mois_annee2")\
             .select("VAR.*", "VAR_REM.SALAIRE_EQ_TP", "VAR_REM.Salaire", "VAR_REM.salaire_bas", "VAR_REM.salaire_moyen", "VAR_REM.salaire_eleve",  "VAR_REM.Augmentation", "VAR_REM.augmentation_recente", "VAR_REM.augmentation_ancienne")

for v in ["SALAIRE_EQ_TP", "salaire_bas", "salaire_moyen", "salaire_eleve", "augmentation_recente", "augmentation_ancienne"]:
  VAR_REM = VAR_REM.withColumn(v, F.when(F.col("SALAIRE_EQ_TP")==0, None).otherwise(F.col(v)))
  VAR_REM = fill_nulls_2sides(VAR_REM, v)

VAR_REM = VAR_REM.fillna(1, "augmentation_recente")
VAR_REM = VAR_REM.fillna(0, "augmentation_ancienne")

if filiale_name == "aerolis":
  VAR_REM = VAR_REM.withColumn("Augmentation", F.lit("donnee manquante"))\
                          .withColumn("augmentation_recente", F.lit(0))\
                          .withColumn("augmentation_ancienne", F.lit(0))
display(VAR_REM.groupBy("Salaire").count())



# COMMAND ----------

# DBTITLE 1,Calcul des variables sur les objets majoration
VAR_MAJ = VAR_REM
VAR_MAJ = VAR_MAJ.withColumn("Auxiliaire", F.when((F.lower(F.trim(F.col("service_agent_origine")))=="aux") | (F.lower(F.trim(F.col("service_agent_id")))=="aux"), 1).otherwise(0))


  
# ANALYSE MAJORATION PENDANT ABS
NIV_MAJ_DICT = {
  'PAS_DE_MAJO': (F.col("majorations") == 0),
  'MAJO_NON_NULLE': (F.col("majorations") > 0),
}

agent_temp = VAR_MAJ.select(F.col("id_salarie").alias('id_salarie2'),
                     F.col("date_roulement").alias("date_roulement_2"),
                     F.col("majorations").alias("majorations2")).distinct()


agent_majo_histo = VAR_MAJ.join(agent_temp, VAR_MAJ.id_salarie == agent_temp.id_salarie2, how='left')\
                                            .filter((agent_temp.date_roulement_2 >= F.add_months(VAR_MAJ['date_roulement'], -1)) 
                                                    & (agent_temp.date_roulement_2 < VAR_MAJ.date_roulement))\
                                            .groupBy(VAR_MAJ.id_salarie,
                                                     VAR_MAJ.date_roulement)\
                                            .agg(F.mean(agent_temp.majorations2).alias("MAJO_AVG_HISTO"),
                                                 F.sum(agent_temp.majorations2).alias("MAJO_SUM_HISTO"))
df1= VAR_MAJ.alias('df1')
df2 = agent_majo_histo.alias('df2')
VAR_MAJ = df1.join(df2, (df1.date_roulement==df2.date_roulement) &
                        (df1.id_salarie==df2.id_salarie), how='left')\
             .select('df1.*', 'df2.MAJO_AVG_HISTO', 'df2.MAJO_SUM_HISTO')

NIV_MAJHIST_DICT = {
  'majorations_mois_precedent_eleve': (F.col("MAJO_AVG_HISTO") >= 0.25),
}

# testq = VAR_MAJ.filter(F.col("MAJO_AVG_HISTO") > 0)\
#                                 .approxQuantile("MAJO_AVG_HISTO", [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99], 0) # [0.02, 0.06, 0.54, 1.0, 1.88, 5.5, 6.68]
# print(testq) # [0.0032258064516129032, 0.03, 0.20400000000000001, 0.3385714285714286, 0.5853333333333333, 1.2913333333333337, 2.1119354838709676]

col = "NIV_MAJO_AVG_HISTO_ABS"
VAR_MAJ = VAR_MAJ.withColumn(col, F.lit(None))
cat_list = []
for name, conds in NIV_MAJHIST_DICT.items():
      cat_list.append(name)
      VAR_MAJ = VAR_MAJ.withColumn(col, F.when(conds, name).otherwise(F.col(col)))

VAR_MAJ = binarize(VAR_MAJ, col, "", cat_list, False)
VAR_MAJ = VAR_MAJ.drop("MAJO_AVG_HISTO", "MAJO_SUM_HISTO", "NIV_MAJO_AVG_HISTO_ABS")


# display(VAR_MAJ.filter(F.col("FLAG_AUXILIAIRE")>0))

# COMMAND ----------

VAR_MAJ = cache(VAR_MAJ, "VAR_MAJ.csv", False)

# COMMAND ----------

# DBTITLE 1,Retards et primes
VAR_PRI = VAR_MAJ

PRI2 = PRI.groupBy("id_salarie", "date_roulement").agg(F.sum("montant_prime").alias("montant_prime"))
df1 = VAR_PRI.alias('df1')
df2 = PRI2.alias('df2')

VAR_PRI = df1.join(df2, (df1.date_roulement == df2.date_roulement) & (df1.id_salarie == df2.id_salarie), how='left')\
             .select("df1.*", "df2.montant_prime")

# df3 = RET.alias('df3')
# df4 = VAR_PRI.alias('df4')
# # display(df4)
# VAR_PRI = df4.join(df3, (df4.date_roulement == df3.date_roulement) & (df4.id_salarie == df3.id_salarie), how='left')\
#              .select("df4.*", "df3.retard")

VAR_PRI = VAR_PRI.fillna(0, subset=["montant_prime"])
# VAR_PRI = VAR_PRI.fillna("na", subset=["lib_prime"])


agent_temp1 = VAR_PRI.select("id_salarie", "date_roulement").distinct()

agent_temp = VAR_PRI.select(F.col("id_salarie").alias('id_salarie2'),
                           F.col("date_roulement").alias("date_roulement_2"),
                           F.col("montant_prime").alias("montant_prime2")).distinct()

# display(agent_temp.select("id_salarie2", "date_roulement_2", "montant_prime2").orderBy("id_salarie2", "date_roulement_2"))

agent_histo = agent_temp1.join(agent_temp, agent_temp1.id_salarie == agent_temp.id_salarie2, how='left')\
                                            .filter((agent_temp.date_roulement_2 > F.add_months(agent_temp1['date_roulement'], -10)) 
                                                    & (agent_temp.date_roulement_2 <= agent_temp1.date_roulement))\
                                            .groupBy(agent_temp1.id_salarie,
                                                     agent_temp1.date_roulement)\
                                            .agg(F.avg(agent_temp.montant_prime2).alias("prime_histo"), F.max(agent_temp.montant_prime2).alias("prime_histo_max"))
# display(agent_histo.select("id_salarie", "date_roulement", "prime_histo_max", "prime_histo"))
agent_histo = agent_histo.withColumn("prime_annee_glissante", F.when(F.col("prime_histo") > 4, "elevee").otherwise("faible"))

df1= VAR_PRI.alias('df1')
df2 = agent_histo.alias('df2')
VAR_PRI = df1.join(df2, (df1.date_roulement==df2.date_roulement) &
                        (df1.id_salarie==df2.id_salarie), how='left')\
             .select('df1.*', 'df2.prime_annee_glissante', "df2.prime_histo")

VAR_PRI = binarize(VAR_PRI, "prime_annee_glissante", "prime", ["elevee", "faible"])
# export_result_df(VAR_PRI.select("id_salarie", "date_roulement", "prime_histo"), "test.csv")
# testq = agent_histo.filter(F.col("prime_histo") > 0)\
#                                 .approxQuantile("prime_histo", [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99], 0) # [0.02, 0.06, 0.54, 1.0, 1.88, 5.5, 6.68]
# print(testq) # [0.0032258064516129032, 0.03, 0.20400000000000001, 0.3385714285714286, 0.5853333333333333, 1.2913333333333337, 2.1119354838709676]

# COMMAND ----------

# DBTITLE 1,Calcul des variables sur les objets d'accidentologie - Travail et Véhicule
ACC_VAR = VAR_PRI

VAR = VAR_PRI

#Import fichier accidentologie : corporels et materiel ######################################################################################################################################

accidentTravail = read_csv("accidentTravail.csv")
accidentVehicule = read_csv("accidentVehicule.csv")

ATR = accidentTravail.alias('ATR')
AVE = accidentVehicule.alias('AVE')

ATR = ATR.withColumn('Date_de_creation', F.to_timestamp(F.col('Date_de_creation'),'yyyy-MM-dd HH:mm'))\
          .withColumn('Date_de_modification', F.to_timestamp(F.col('Date_de_modification'),'yyyy-MM-dd HH:mm'))\
          .withColumn('Date_de_declaration', F.to_date(F.col("Date_de_declaration"),"yyyy-MM-dd"))\
          .withColumn('Date_de_l_evenement', F.to_date(F.col("Date_de_l_evenement"),"yyyy-MM-dd"))\
          .withColumn('Date_de_premiere_constatation', F.to_date(F.col("Date_de_premiere_constatation"),"yyyy-MM-dd"))\
          .withColumn('Date_de_fin_de_l_arret', F.to_date(F.col("Date_de_fin_de_l_arret"),"yyyy-MM-dd"))\
          .withColumn('Date_de_depart_de_l_arret', F.to_date(F.col("Date_de_depart_de_l_arret"),"yyyy-MM-dd"))\
          .withColumn('Date_de_refus_de_PEC', F.to_date(F.col("Date_de_refus_de_PEC"),"yyyy-MM-dd"))\
          .withColumn('Date_du_deces', F.to_date(F.col("Date_du_deces"),"yyyy-MM-dd"))\
          .withColumn('Date_de_guerison', F.to_date(F.col("Date_de_guerison"),"yyyy-MM-dd"))\
          .withColumn('Date_de_contestation', F.to_date(F.col("Date_de_contestation"),"yyyy-MM-dd"))\
          .withColumn('Date_de_l_Enquete', F.to_date(F.col("Date_de_l_Enquete"),"yyyy-MM-dd"))

AVE = AVE.withColumn('date_accident',F.to_date(F.col("date_accident"),"yyyy-MM-dd"))

#SIN = read_csv("sinistre.csv")
#SIN = SIN.withColumn('date_heure',F.to_timestamp(F.col("date_heure"),"yyyy-MM-dd HH:mm"))

############################################################################################################################################################################################

#Jointure entre le fichier des variables calculées (roulements) et [calendrier, météo] #####################################################################################################

AVE_VAR = AVE.groupBy('date_accident').count()\
             .withColumnRenamed('count', 'nb_acc_mat_by_date')

ag_roul4 = ACC_VAR.alias('ag_roul4')
acc_mat = AVE_VAR.alias('acc_mat')
ACC_VAR = ag_roul4.join(acc_mat,ag_roul4.date_roulement==acc_mat.date_accident, how='left')\
                  .select('ag_roul4.*','acc_mat.nb_acc_mat_by_date')

# Jointure entre les agent_roulement_perim et les accidents corporels -> pour les interactions de surlignages temporels
ATR_VAR = ATR.groupBy("Date_de_l_evenement").agg(F.countDistinct(F.col("Numero_de_dossier")))\
                                            .withColumnRenamed('count(DISTINCT Numero_de_dossier)', 'nb_acc_corp_by_date')

ag_roul5 = ACC_VAR.alias('ag_roul5')
acc_corp = ATR_VAR.alias('acc_corp')
ACC_VAR = ag_roul5.join(acc_corp,ag_roul5.date_roulement==acc_corp.Date_de_l_evenement, how='left')\
                  .select('ag_roul5.*','acc_corp.nb_acc_corp_by_date')
#SIN_VAR = SIN.withColumn("date",F.to_date("date_heure",'yyyy-MM-dd'))\
#             .groupBy("date").agg(F.countDistinct(F.col("num_dossier")))\
#             .withColumnRenamed('count(DISTINCT num_dossier)', 'nb_acc_sin_by_date')

#ag_roul6 = ACC_VAR.alias('ag_roul6')
#acc_sin = SIN_VAR.alias('acc_sin')
#ACC_VAR = ag_roul6.join(acc_sin,ag_roul6.date_roulement==acc_sin.date, how='left')\
#                  .select('ag_roul6.*','acc_sin.nb_acc_sin_by_date')

############################################################################################################################################################################################

#Création variable ACC_CORP_WINDOW, ACC_MAT_WINDOW & ACC_WINDOW ############################################################################################################################

Q90_acc_corp = ATR_VAR.approxQuantile("nb_acc_corp_by_date", [0.95],0)
Q90_acc_mat = AVE_VAR.approxQuantile("nb_acc_mat_by_date", [0.95],0)
#Q90_acc_sin = accident_sinistre_date.approxQuantile("nb_acc_sin_by_date", [0.95],0)

acc_corp = ATR_VAR.alias('acc_corp')
acc_mat = AVE_VAR.alias('acc_mat')
#acc_sin = SIN_VAR.alias('acc_sin')

cal = calendrier.alias('cal')

accident_corp_date = cal.join(acc_corp, cal.date == acc_corp.Date_de_l_evenement,how='left').select('cal.date','acc_corp.nb_acc_corp_by_date')
acc_corp_ref = accident_corp_date.alias('acc_corp_ref')


#Remplace les requète du dessous ###########################

accident_mat_date = cal.join(acc_mat, cal.date == acc_mat.date_accident,how='left')\
                       .select('cal.date','acc_mat.nb_acc_mat_by_date')\
                       .withColumnRenamed('date', 'date2')
                                              
acc_mat_ref = accident_mat_date.alias('acc_mat_ref')

accident_corp_mat_date = acc_corp_ref.join(acc_mat_ref, acc_corp_ref.date == acc_mat_ref.date2, how='left')\
                                     .select('acc_corp_ref.*','acc_mat_ref.nb_acc_mat_by_date')\
                                     .withColumn("ACC_CORP_WINDOW", F.when(F.col("nb_acc_corp_by_date")>=Q90_acc_corp[0],1).otherwise(0))\
                                     .withColumn("ACC_MAT_WINDOW", F.when(F.col("nb_acc_mat_by_date")>=Q90_acc_mat[0],1).otherwise(0))

windowPost = Window.orderBy("date").rowsBetween(-2,Window.currentRow)

accident_corp_mat_date = accident_corp_mat_date.withColumn("Bcp_accidents_corporels_3J", F.when((F.sum(F.col("ACC_CORP_WINDOW")).over(windowPost)>0) & (F.col("ACC_CORP_WINDOW")==0) ,1).otherwise(0))\
                                               .withColumn("Bcp_accidents_materiels_3J", F.when((F.sum(F.col("ACC_MAT_WINDOW")).over(windowPost)>0) & (F.col("ACC_MAT_WINDOW")==0)  ,1).otherwise(0))\
                                               .withColumn("Bcp_accidents_3J", F.when( ((F.col("ACC_CORP_WINDOW")==1) | (F.col("ACC_MAT_WINDOW")==1)) ,1).otherwise(0))

acc_mat_corp2 = accident_corp_mat_date.alias('acc_mat_corp2')
ag_roul_id4 = VAR.alias('ag_roul_id4')

ACC_VAR = ag_roul_id4.join(acc_mat_corp2,ag_roul_id4.date_roulement==acc_mat_corp2.date, how='left')\
                     .select('ag_roul_id4.*','acc_mat_corp2.Bcp_accidents_corporels_3J','acc_mat_corp2.Bcp_accidents_materiels_3J', 'acc_mat_corp2.Bcp_accidents_3J')

ACC_VAR = ACC_VAR.drop("nb_acc_mat_by_date", "nb_acc_corp_by_date")

############################################################

############################################################

#accident_corp_mat_sin_date = acc_mat_corp.join(acc_sin,acc_mat_corp.date == acc_sin.date,how='left').select('acc_mat_corp.*','acc_sin.nb_acc_sin_by_date')
#acc_mat_corp_sin = accident_corp_mat_sin_date.alias('acc_mat_corp_sin')

#accident_corp_mat_sin_date = acc_mat_corp_sin.join(acc_mat,acc_mat_corp_sin.date == acc_mat.date_accident,how='left').select('acc_mat_corp_sin.*','acc_mat.nb_acc_mat_by_date')\
#                          .withColumn("ACC_CORP_WINDOW", F.when(F.col("nb_acc_corp_by_date")>=Q90_acc_corp[0],1).otherwise(0))\
#                          .withColumn("ACC_MAT_WINDOW", F.when(F.col("nb_acc_mat_by_date")>=Q90_acc_mat[0],1).otherwise(0))\
#                          .withColumn("ACC_SIN_WINDOW", F.when(F.col("nb_acc_sin_by_date")>=Q90_acc_sin[0],1).otherwise(0))

#my_window_post = Window.orderBy("date").rowsBetween(-2,Window.currentRow)
#accident_corp_mat_sin_date = accident_corp_mat_sin_date.withColumn("ACC_CORP_WINDOW", F.when((F.sum(F.col("ACC_CORP_WINDOW")).over(my_window_post)>0) & (F.col("ACC_CORP_WINDOW")==0) ,1).otherwise(0))\
#                                  .withColumn("ACC_MAT_WINDOW", F.when((F.sum(F.col("ACC_MAT_WINDOW")).over(my_window_post)>0) & (F.col("ACC_MAT_WINDOW")==0)  ,1).otherwise(0))\
#                                  .withColumn("ACC_SIN_WINDOW", F.when((F.sum(F.col("ACC_SIN_WINDOW")).over(my_window_post)>0) & (F.col("ACC_SIN_WINDOW")==0)  ,1).otherwise(0))\
#                                  .withColumn("ACC_WINDOW", F.when( ((F.col("ACC_CORP_WINDOW")==1) | (F.col("ACC_MAT_WINDOW")==1) | (F.col("ACC_SIN_WINDOW")==1)) ,1).otherwise(0))

#acc_mat_corp2 = accident_corp_mat_sin_date.alias('acc_mat_corp2')
#ag_roul_id4 = ACC_VAR.alias('ag_roul_id4')
#ACC_VAR = ag_roul_id4.join(acc_mat_corp2,ag_roul_id4.date_roulement==acc_mat_corp2.date, how='left')\
#                     .select('ag_roul_id4.*','acc_mat_corp2.ACC_WINDOW','acc_mat_corp2.ACC_MAT_WINDOW', 'acc_mat_corp2.ACC_CORP_WINDOW', 'acc_mat_corp2.ACC_SIN_WINDOW' )

#ACC_VAR = ACC_VAR.drop("nb_acc_mat_by_date", "nb_acc_corp_by_date", "nb_acc_sin_by_date")

############################################################

############################################################################################################################################################################################
# ACC_VAR = bcache(ACC_VAR)


# COMMAND ----------

# DBTITLE 1,VAR ANALYSES
import pandas as pd
import numpy as np

VAR_FULL = ACC_VAR

# VAR_FULL_TREATMENT = fill_nulls_2sides(VAR_FULL, "CENTRE_LIBELLE")
# VAR_FULL_TREATMENT = fill_nulls_2sides(VAR_FULL_TREATMENT, "LIGNE_LIBELLE")
# VAR_FULL_TREATMENT = VAR_FULL_TREATMENT.withColumn("date_roulement", agent_roulement_perim_exploit.date_roulement.cast("date"))\
#                                        .withColumn("DATE_DEBUT", agent_roulement_perim_exploit.DATE_DEBUT.cast("date"))\
#                        peut                 .withColumn("DATE_FIN", agent_roulement_perim_exploit.DATE_FIN.cast("date"))

# VAR_FULL = fill_nulls_2sides(VAR_FULL, "lib_depot")
# VAR_FULL = fill_nulls_2sides(VAR_FULL, "lib_ligne")

# VAR_FULL_TREATMENT = binarize(VAR_FULL_TREATMENT, "CENTRE_LIBELLE", "CENTRE_AV_SERVICE", centre_lib_list)

# cat_pro_list = ["MAITRISE", "EMPLOYE", "HAUTE MAITRISE", "OUVRIER", "CONDUCTEUR", "CADRE"]

VAR_FULL = binarize(VAR_FULL, "lib_categorie_professionnelle", "CAT_PRO", CON_CP)
VAR_FULL = binarize(VAR_FULL, "lib_depot_add", "DEPOT", DEP_VAR)

VAR_FULL = VAR_FULL.fillna("donnee manquante", subset=["lib_depot_add"])

VAR_FULL = fill_nulls_2sides(VAR_FULL, "type_voiture")
# VAR_FULL = binarize(VAR_FULL, "type_voiture", "Type_voiture")


dictVarT = [
  ("AGE", ["Age_moins_26", "Age_26_30", "Age_31_40", "Age_41_50", "Age_51_60", "Age_plus_60"]),
  ("SALARIE", ['Homme', 'Femme', "Genre_non_fourni", "En_couple", "Celibataire"]),
  ("ENFANTS",['0_enfant_moins_20ans','1_enfant_moins_20ans', "2plus_enfants_moins_20ans"]),
  ("ANCIENNETE", [  "Anciennete_moins_1an", "Anciennete_1an_5ans", "Anciennete_6ans_10ans", "Anciennete_11ans_20ans","Anciennete_21ans_30ans","Anciennete_plus_30ans"]),
  
  ("CALENDRIER", ["Jour_Ferie_2J", "Jour_Religieux_2J", "Periode_Grippe", "Periode_Gastro", "Periode_Epidemie", 
                       "Periode_vacances_scolaires", "Periode_neige", "Periode_temperatures_extremes", "Weekend", "Jour_ouvre", "Vendredi", "Lundi"]),
  ("CONTRAT", ["Temps_partiel", "Stage", "CDI", "CDD_vacation", "CDD", "CPS", "Auxiliaire"]),
  ("CATEGORIE_PRO", get_cols_starting_with(VAR_FULL, "CAT_PRO")),
  ("SALAIRES", [ 'salaire_bas', 'salaire_moyen', 'salaire_eleve']),
#   "08_AUGMENTATION": ['augmentation_recente', 'augmentation_ancienne'],
  ("PRIMES", ['prime_elevee', "prime_faible"]),
  ("MAJORATION", ["majorations_mois_precedent_eleve"]),
#   "10_AUXILAIRE": ["FLAG_AUXILIAIRE"],
   
  ("ACCIDENT", ['Bcp_accidents_corporels_3J', 'Bcp_accidents_materiels_3J', 'Bcp_accidents_3J']),

  ("MODE", ["Tramway"]),
  ("TRAME", ["Bcp_soirees"]),

  ("HORAIRE_SERVICES" , ["Bcp_debut_service_tot", "Bcp_fin_service_tard"]),
  ("AMPLITUDE_HORAIRE" , ["Bcp_faibles_amplitudes", "Bcp_grandes_amplitudes"]),
  ("TEMPS_TRAJET", get_cols_starting_with(VAR_FULL, "temps_trajet")),
  ("CENTRE", get_cols_starting_with(VAR_FULL, "DEPOT")),
  ("LIGNE", ["lib_ligne"]),
  ("TYPE_VOITURE", get_cols_starting_with(VAR_FULL, "Type_voiture")),
]

catToOrder = ["AGE",  "ANCIENNETE", "CALENDRIER", "SALAIRES", "ENFANTS"]
dictVar = {}
orderRename = {}

i = 1
for k, v in dictVarT:
  new_col = "{:02d}_{}".format(i, k)
  i += 1
  dictVar[new_col] = v
  if k in catToOrder:
    j = 1
    for e in dictVar[new_col]:
      orderRename[e] = "{:02d}_{}".format(j, e)
      j += 1

listDuree = ["1 jour", "2 à 3 jours", "4 à 7 jours", "8 jours à 1 mois", "1 à 3 mois", "plus de 3 mois"]
print(dictVar)

# COMMAND ----------

# DBTITLE 1,Quick test pour Chi2
# test variables:
# v = 'DEPOT_AV_SERVICE_arnas-69655-69-rue-du-champ-du-garet'
# my_window = Window.partitionBy("FLAG_ABS", v)
# tt = VAR_FULL.withColumn("COUNT", F.count("FLAG_ABS").over(my_window))\
#              .select("FLAG_ABS", v, "COUNT").distinct().orderBy("FLAG_ABS", v)
# display(tt)
# print(CON_CP)
# print(VAR_REM.filter(F.col("type_duree").isNull()).count())

# COMMAND ----------

# DBTITLE 1,KHI2
#Chi2Test - Convert to Pandas DataFrame #######################################################################################################################################
VAR_FULL_COLS = VAR_FULL.schema.names

working_cols = ["FLAG_ABS", "type_duree", "id_salarie"]
working_cols +=  sum(dictVar.values(), [])
working_cols = [c for c in working_cols if c in VAR_FULL_COLS]

dd = VAR_FULL.filter(F.col("date_roulement")==F.col("DATE_DEBUT")).select(*working_cols)
df = dd.toPandas()
df["type_duree"] = df["type_duree"].fillna("_")

if filiale_name == "besancon":
   df = pd.concat([df]*4, ignore_index=True)
if filiale_name == "autocarPlanche":
   df = pd.concat([df]*2, ignore_index=True)
if filiale_name == "aerolis":
   df = pd.concat([df]*2, ignore_index=True)
    
prefix_delete = ["DEPOT_", "CAT_PRO_", "Type_voiture_"]
from scipy.stats import chi2_contingency

df_base = pd.DataFrame([{"a":0, "b":0, "c": 0},
                        {"a":0, "b":1, "c": 0},
                        {"a":1, "b":0, "c": 0},
                        {"a":1, "b":1, "c": 0}])

def run_chi2_test(VarATester, TypeDuree, cat, contingency_matrix, effectif_tot_var, save_cat=False):
    result_dict = {'TYPE_DUREE': TypeDuree, 'VARIABLE': VarATester, "CATEGORIE": cat, 'EFFECTIF_VAR': effectif_tot_var}
    result_dict["VARIABLE_ini"] = VarATester
    if VarATester in orderRename:
      result_dict["VARIABLE"] = orderRename[VarATester]
      
    for e in prefix_delete:
      result_dict["VARIABLE"] = result_dict["VARIABLE"].replace(e, "")
    
    result_dict["VARIABLE"] = "{} ({})".format(result_dict["VARIABLE"], effectif_tot_var)
      
#       print(result_dict)
    df_base_t = df_base.copy()      
    df_base_t.columns = [VarATester, "FLAG_ABS", "Effectif"]
    df_base_t.set_index([VarATester, "FLAG_ABS"], inplace=True)

    contingency_matrix = contingency_matrix.reindex(df_base_t.index).fillna(0.0).reset_index()
    contingency_matrix.columns = [VarATester, "FLAG_ABS", "Effectif"]

    if (len(contingency_matrix) != 4):
      print(VarATester, TypeDuree, "\t ==> Erreur : degre de liberte inattendu", values)
      return []

    values = contingency_matrix["Effectif"].tolist()#contingency_matrix.rdd.flatMap(lambda x : x).collect()

    if (values[1]<20.0) | (values[3]<20.0):
      print(VarATester, TypeDuree, "\t ==> Erreur : Effectif cible trop faible -> inferieur à 20", values)
      result_dict['TEST'] = "=="
      result_dict['Effectif_tot_var_0'] = 1
      result_dict['Effectif_cible_var_0'] = 1
      result_dict['Effectif_tot_var_1'] = 1
      result_dict['Effectif_cible_var_1'] = 1
      return []

    if (values[0] == 0) | (values[2] == 0):
      print(VarATester, TypeDuree, "\t ==> Erreur : Effectif null -> inferieur à 20", values)
      result_dict['TEST'] = "=="
      result_dict['Effectif_tot_var_0'] = 1
      result_dict['Effectif_cible_var_0'] = 1
      result_dict['Effectif_tot_var_1'] = 1
      result_dict['Effectif_cible_var_1'] = 1
      return []

    dm = Matrices.dense(2, 2, values)
    independenceTestResult = Statistics.chiSqTest(dm)

    result_dict['Effectif_tot_var_0'] = values[0]
    result_dict['Effectif_cible_var_0'] = values[1]
    result_dict['Effectif_tot_var_1'] = values[2]
    result_dict['Effectif_cible_var_1'] = values[3]
    
    
    
    ratio_varATester_0 = values[1] / values[0]          
    ratio_varATester_1 = values[3] / values[2] 


    CibleSup = ratio_varATester_0 <= ratio_varATester_1
    if independenceTestResult.pValue <= 0.01:
      result_dict['TEST'] = "+++" if CibleSup else "---"
      result_dict['TEST_NUM'] = 3 if CibleSup else -3
      if save_cat:
        var_for_dec_tree.add(VarATester)
    elif independenceTestResult.pValue <= 0.05:
      result_dict['TEST'] = "++" if CibleSup else "--"
      result_dict['TEST_NUM'] = 2 if CibleSup else -2

      if save_cat:
        var_for_dec_tree.add(VarATester)
    elif independenceTestResult.pValue <= 0.1:
      result_dict['TEST'] = "+" if CibleSup else "-"
      result_dict['TEST_NUM'] = 1 if CibleSup else -1

      if save_cat:
        var_for_dec_tree.add(VarATester)
    else:
      result_dict['TEST'] = "=="
      result_dict['TEST_NUM'] = 0

    return [result_dict]

lignes_planche = ['LED', 'K0103', 'K0102', 'K0101']
lignes_transpole = ['Ligne Saint Exupery', 'Corolle', 'Liane 04', 'Liane 2', 'missing', 'Ligne Citadine', 'Ligne Moliere', 'Liane 03', 'Citadine 5', 'Citadine Tourcoing', 'Citadine Seq']

var_for_dec_tree = set()
results = []
categorical_variables = ["lib_ligne"]
for cat, listVar in dictVar.items():
  print("# " + cat)
  for VarATester in listVar:
    if VarATester not in df.columns:
      continue
    if VarATester in categorical_variables: 
        col_values = list(set(df[VarATester]))
        for c in col_values:
          
          if (filiale_name == "autocarPlanche") & (c not in lignes_planche):
            continue
          if (filiale_name == "transpole") & (c not in lignes_transpole):
            continue
          df['cat_var'] = 0
          df.loc[df[VarATester]==c, 'cat_var'] = 1
          
          tt = df.loc[df[VarATester]==c, 'id_salarie'].nunique()
          for TypeDuree in listDuree:
              msk = ((df["type_duree"] == TypeDuree) | (df["FLAG_ABS"] == 0)) & df['cat_var'].notnull()
              mat = df[msk].groupby(['cat_var', "FLAG_ABS"])["type_duree"].count().reset_index()
              mat.set_index(['cat_var', "FLAG_ABS"], inplace=True)
              ret = run_chi2_test(c, TypeDuree, cat, mat, tt, True)
              results += ret
    else:
      tt = df.loc[df[VarATester]==1, 'id_salarie'].nunique()
      for TypeDuree in listDuree:
        msk = ((df["type_duree"] == TypeDuree) | (df["FLAG_ABS"] == 0)) & df[VarATester].notnull()
        mat = df[msk].groupby([VarATester,"FLAG_ABS"])["type_duree"].count().reset_index()
        mat.set_index([VarATester, "FLAG_ABS"], inplace=True)
        ret = run_chi2_test(VarATester, TypeDuree, cat, mat, tt)
        results += ret

df_results = spark.createDataFrame(results)
df_results = df_results.drop('TEST_NUM', "VARIABLE_ini")

df_results.withColumn("cat_duree", F.when(F.col("type_duree").isin(["1_1jour", "2_<=3jours", "3_<=1semaine"]), "Absences courtes").otherwise("Absences longues"))

df_results_num = pd.DataFrame(results)
df_results_num = df_results_num.groupby("VARIABLE_ini")["TEST_NUM"].sum().reset_index()

export_result_df(df_results, "result_chi2_tests.csv", encoding="UTF-8")


# COMMAND ----------

# df_results = pd.DataFrame(results)
# df_results["cat_duree"] = "Absences longues"
# df_results.loc[df_results["TYPE_DUREE"].isin(["1_1jour", "2_<=3jours", "3_<=1semaine"]), "cat_duree"] = "Absences courtes"
# df_results = df_results.groupby(["CATEGORIE", "VARIABLE", "cat_duree"])["TEST_NUM"].sum().reset_index()

# df_results.loc[df_results["TEST_NUM"]>0, "explique"] = "ABSENCE"
# df_results.loc[df_results["TEST_NUM"]<0, "explique"] = "PRESENCE"
# df_results.loc[df_results["TEST_NUM"]==0, "explique"] = "RIEN"
# out_filename = os.path.join("/dbfs" + result_path, 'result_var_expl.csv')
# df_results.to_csv(out_filename, sep=";", decimal='.', index=False)


# print(df_results)


# COMMAND ----------

# DBTITLE 1,ARBRE DE DECISION
from copy import deepcopy
from sklearn import tree
import numpy as np

VAR_FULL_ML = VAR_FULL

dictVarML = deepcopy(dictVar)

calkey = [e for e in dictVarML if "CALENDRIER" in e][0]

dictVarML.pop(calkey)
# dictVarML["07_SALAIRES"] =  ['salaire_bas']
# dictVarML["08_AUGMENTATION"] =  []
# dictVarML["14_HORAIRE_SERVICES"] =  []
# dictVarML["15_AMPLITUDE_HORAIRE"] =  []

filters = [ "duree_cumul", "index", "type_duree"]

inv_dictVar = {}
for k, vlist in dictVarML.items():
  for v in vlist:
    inv_dictVar[v] = k
    
VAR_FULL_COLS = VAR_FULL_ML.schema.names

if ("lib_ligne" in VAR_FULL_COLS) & ("lib_ligne" in inv_dictVar):
  print("yes")
  VAR_FULL_ML = binarize(VAR_FULL_ML, "lib_ligne", "LIGNE_AV_SERVICE", var_for_dec_tree)
  calkey = [e for e in dictVarML if "LIGNE" in e][0]

  dictVarML[calkey] = get_cols_starting_with(VAR_FULL_ML, "LIGNE_")
features_to_use = sum(dictVarML.values(), ["FLAG_ABS"])
features_to_use = [c for c in features_to_use if c in VAR_FULL_COLS]

VAR_FULL_ML_IDX = VAR_FULL_ML.withColumn("index", F.concat(F.lit("ID"), F.monotonically_increasing_id()).cast("string"))
VAR_FULL_ML_IDX = cache(VAR_FULL_ML_IDX, "VAR_FULL_ML_IDX.csv", False)
df = VAR_FULL_ML_IDX.select(*features_to_use, *filters).toPandas()

df_train = df[~df["type_duree"].isin(["5_<=3mois", "6_>3mois"])]
df_train = df.drop(filters, axis=1)
df_test = df.drop(filters, axis=1)
df_test_ids = df[["index"]]

print("DATASET: ", len(df_train))
print("DATASET1: ", len(df_train[df_train["FLAG_ABS"]==1]))
print("DATASET0: ", len(df_train[df_train["FLAG_ABS"]==0]))

# COMMAND ----------

target_cols = "FLAG_ABS"
target = df_train[target_cols].fillna(0).values
features_cols = [c for c in features_to_use if c != "FLAG_ABS"]
features = df_train[features_cols].fillna(0).values

if filiale_name == "besancon":
  min_samples_leaf=200
elif filiale_name == "transpole":
  min_samples_leaf=5000
elif filiale_name == "autocarPlanche":
  min_samples_leaf=1500
elif filiale_name == "aerolis":
  min_samples_leaf=200
  
clf = tree.DecisionTreeClassifier(min_samples_leaf=min_samples_leaf)
clf = clf.fit(features, target)

feature_importance = list(zip(features_cols, clf.feature_importances_))
feature_importance = sorted(feature_importance, key=lambda tup: tup[1], reverse=True)
df_feat_imp = pd.DataFrame(feature_importance, columns=["feature", "importance"])

# df_results_num = df_results_num.rename({"VARIABLE": "feature"})
df_results_num["feature"] = df_results_num["VARIABLE_ini"]

invert = {"Homme": "Femme", "Femme": "Homme", "En_couple": "Celibataire", "Celibataire": "En_couple", "Age_plus_50": "Age_moins_50", "Age_moins_30": "Age_plus_30",}

df_feat_imp = df_feat_imp.merge(df_results_num[["feature", "TEST_NUM"]], on="feature", how='left')

df_feat_imp.loc[df_feat_imp["TEST_NUM"]<0, "feature2"] = df_feat_imp.loc[df_feat_imp["TEST_NUM"]<0, "feature"].map(invert)
df_feat_imp.loc[df_feat_imp["feature2"].notnull(), "feature"] = df_feat_imp.loc[df_feat_imp["feature2"].notnull(), "feature2"] 
df_feat_imp.loc[df_feat_imp["feature2"].notnull(), "TEST_NUM"] = -1*df_feat_imp.loc[df_feat_imp["feature2"].notnull(), "TEST_NUM"] 
df_feat_imp["importance"] = (df_feat_imp["importance"] *100).astype(int)
df_feat_imp = df_feat_imp[(df_feat_imp["TEST_NUM"]>0) & (df_feat_imp["importance"]>0)]
out_filename = os.path.join("/dbfs" + result_path, 'results_features.csv')
df_feat_imp[["feature", "importance"]].to_csv(out_filename, sep=";", decimal='.', index=False)
print(df_feat_imp)


# COMMAND ----------

n_nodes = clf.tree_.node_count
children_left = clf.tree_.children_left
children_right = clf.tree_.children_right
feature = clf.tree_.feature
threshold = clf.tree_.threshold

node_depth = np.zeros(shape=n_nodes, dtype=np.int64)
is_leaves = np.zeros(shape=n_nodes, dtype=bool)
stack = [(0, -1)]  # seed is the root node id and its parent depth
while len(stack) > 0:
    node_id, parent_depth = stack.pop()
    node_depth[node_id] = parent_depth + 1

    # If we have a test node
    if (children_left[node_id] != children_right[node_id]):
        stack.append((children_left[node_id], parent_depth + 1))
        stack.append((children_right[node_id], parent_depth + 1))
    else:
        is_leaves[node_id] = True

print(len(feature))                    

#######################
# Applying the model to the full dataset
#######################

idx = df_test_ids["index"].values
target = df_test[target_cols].fillna(0).values
features = df_test[features_cols].fillna(0).values

#######################
# path by leaf 
#######################

paths_each_node = {}
path = clf.decision_path(features)
predict = clf.predict(features)
pathtocoo = path.tocoo()
path_df = pd.DataFrame.from_dict({"feat": list(pathtocoo.row), "node": list(pathtocoo.col)})
path_df = path_df.groupby('feat')["node"].apply(list)
row_leafs = path_df.apply(max)
paths_each_node = list(zip(path_df.values, predict, target))
print(len(row_leafs))
#######################
# stats by leaf
#######################
path_df2 = pd.DataFrame.from_dict({"path": path_df.values, "prediction": predict, "target": target, "leaf_id": row_leafs})
path_df2["predicted1"] = 0
path_df2.loc[path_df2["prediction"]==1, "predicted1"] = 1

path_df2["predicted0"] = 0
path_df2.loc[path_df2["prediction"]==0, "predicted0"] = 1

path_df2["correct"] = 0
path_df2.loc[(path_df2["prediction"]==path_df2["target"]) , "correct"] = 1

path_df2["false"] = 0
path_df2.loc[(path_df2["prediction"]!=path_df2["target"]) , "false"] = 1



path_df2 = path_df2.groupby("leaf_id")["predicted1", "predicted0", "correct", "false"].sum()

# path_df2["abs"] = 0
# path_df2.loc[(path_df2["predicted1"]>0) , "abs"] = path_df2["correct"] / path_df2["predicted1"]
# path_df2.loc[(path_df2["predicted0"]>0) , "abs"] = path_df2["false"] / path_df2["predicted0"]

path_df2["precision"] = path_df2["correct"] / (path_df2["correct"] + path_df2["false"])
print(path_df2[path_df2["predicted1"]>0])

#######################
# MERGING THE NODE ID INTO THE FULL DATASET
#######################
df_nodes_by_row = pd.concat([df_test_ids, row_leafs], axis=1)
spark_df_nodes_by_row = spark.createDataFrame(df_nodes_by_row)

df1 = VAR_FULL_ML_IDX.alias("df1")
df2 = spark_df_nodes_by_row.alias("df2")
VAR_FULL_TREE = df1.join(df2, df1.index==df2.index, how="left").select("df1.*", "df2.node")



# COMMAND ----------

leafs = path_df2.to_dict(orient="index")
#######################
# GENERATING POSSIBLE PATHS
#######################
possible_paths = []
cpt = 0
while len(possible_paths) < len(leafs):
  tmp = paths_each_node[cpt][0]
  cond =[(tmp[i], 
          features_to_use[feature[tmp[i]]+1],
          threshold[tmp[i]],tmp[i+1] == children_left[tmp[i]]) for i in range(0,len(tmp)-1)] + [tmp[-1]]
  if cond not in possible_paths:
    possible_paths.append(cond)
  cpt = cpt + 1

CAT = {
#   "ANCIENNETE": [ "Anciennete_moins_1an", "Anciennete_1an_5ans", "Anciennete_5ans_10ans", "Anciennete_plus_10ans"],
#   "AGE": ['Age_moins_30', 'Age_30_40', 'Age_40_50', 'Age_plus_50'],
  "ENFANTS": ['ss_enfant_moins_20ans','1_enfant_moins_20ans', "au_moins_2_enfant_moins_20ans"],
#   "CENTRE": get_cols_starting_with(VAR_FULL_TREE, "CENTRE_AV_SERVICE"),
#   "LIGNE": get_cols_starting_with(VAR_FULL_TREE, "LIGNE_AV_SERVICE"),
#   "TYPE_CONTRAT": ["Stage","CDI","CDD_vacation","CDD"],
#   "CAT_PRO": get_cols_starting_with(VAR_FULL_TREE, "CAT_PRO"),
#   "DUR_DOM_TRAVAIL": get_cols_starting_with(VAR_FULL_TREE, "temps_trajet"),
  "SALAIRE": ["salaire_bas", "salaire_moyen", "salaire_eleve"],
  "GENRE": ["Homme", "Femme"],
  "STATUT FAMILIAL": ["En_couple", "Celibataire"],
  "AUGMENTATION": ["augmentation_recente", "augmentation_ancienne"],
#   "HORAIRES" : ["Bcp_debut_service_tot", "Bcp_fin_service_tard"],
#   "AMPLITUDES_HORAIRES" : ["Bcp_faibles_amplitudes", "Bcp_grandes_amplitudes"],
#   "TRAME_HORAIRE": ["Bcp_soirees"],
  "TEMPS_TRAJET": ["temps_trajet_inf_15min", "temps_trajet_15_30min", "temps_trajet_sup_30min"]
}
CAT = {}
inv_dictVar = {}
for k, vlist in CAT.items():
  for v in vlist:
    inv_dictVar[v] = k

#######################
# CONSTRUCTING A TABLE WITH RULES AND STATS
#######################
df_list = []
last_cols = ["predicted0", "predicted1", "correct", "false", "precision"]
for i in range(len(possible_paths)):
  path = possible_paths[i]
  node_id = path[-1]
  feat_dict = leafs[node_id]
  feat_dict["node"] = node_id
  for p in path[:-1]:
    feat_dict[p[1]] = not p[3]
  
  compact_feat_dict = {}
  
#   for cat_, vars_ in CAT.items():
#     temps_vars = list(vars_)
#     for v in vars_:
#       if v in feat_dict:
#         if feat_dict[v] is True:
#           compact_feat_dict[cat_] = v.lower().replace(inv_dictVar[v].lower() + "_", "").replace(inv_dictVar[v].lower(), "")
#           break
          
#         if feat_dict[v] is False:
#           temps_vars.remove(v)
#     if len(temps_vars) == 1:
#       v = temps_vars[0]
#       compact_feat_dict[cat_] = v.lower().replace(inv_dictVar[v].lower() + "_", "").replace(inv_dictVar[v].lower(), "")

#   for k, v in feat_dict.items():
#     if k not in inv_dictVar:
#       compact_feat_dict[k] = v        
  
  for k, v in feat_dict.items():
    if k in inv_dictVar:
      if v:
        compact_feat_dict[inv_dictVar[k]] = k.lower().replace(inv_dictVar[k].lower() + "_", "").replace(inv_dictVar[k].lower(), "")
    else:
      compact_feat_dict[k] = v
      

  df_list.append(compact_feat_dict)

#######################
# EXPORT TABLE
#######################
df_tree = pd.DataFrame(df_list)

feat_cols = [c for c in df_tree.columns if c not in last_cols+["node"]]
df_tree = df_tree[["node"] + sorted(feat_cols) + last_cols]


min_sal = {"autocarPlanche": 15, "transpole": 30, "besancon": 10, "aerolis": 10}

def filter_first_lines(predicted):
  dfo = df_tree[df_tree[predicted]>0]
  dfo = dfo.sort_values('precision', ascending=False)
  dfo = dfo.head(15)
  dfo = pd.melt(dfo, id_vars=['node', "nb_salarie"] + last_cols, value_vars=feat_cols, var_name='variable', value_name='value')
  dfo = dfo.dropna()
  return dfo

def filter_first_lines2(isabs=True):
  dfo = df_tree.sort_values('tx_abs', ascending=(not isabs))
  dfo = dfo[dfo["nb_salarie"] > min_sal[filiale_name]]
  dfo = dfo.head(15)
  print(dfo[['node', "nb_salarie", "tx_abs"]])

  dfo = pd.melt(dfo, id_vars=['node', "nb_salarie", "tx_abs"] + last_cols, value_vars=feat_cols, var_name='variable', value_name='value')
  dfo = dfo.dropna()
  return dfo

tt = VAR_FULL_TREE
tt = tt.withColumn("ABS", F.when(F.col("FLAG_ABS")==1, F.col("heures_service") ).otherwise(0))
tt = tt.withColumn("PRE", F.when(F.col("FLAG_ABS")==0, F.col("heures_service") ).otherwise(0))
tt2 = tt.groupBy("node").agg(F.sum("ABS").alias("ABS"), F.sum("PRE").alias("PRE"), F.countDistinct("id_salarie").alias("nb_salarie"))
tt2 = tt2.withColumn("tx_abs", F.col("ABS")/(F.col("ABS")+F.col("PRE")))
df_count_sal = tt2.select("node", "tx_abs", "nb_salarie").toPandas()

df_tree = df_tree.merge(df_count_sal, on='node')

out_filename_abs = os.path.join("/dbfs" + result_path, 'results_tree_abs.csv')
df_tree_out_abs = filter_first_lines2(True)
df_tree_out_abs.to_csv(out_filename_abs, sep=";", decimal='.', index=False)

out_filename_pres = os.path.join("/dbfs" + result_path, 'results_tree_pres.csv')
df_tree_out_pres = filter_first_lines2(False)
df_tree_out_pres.to_csv(out_filename_pres, sep=";", decimal='.', index=False)


dfo = pd.melt(df_tree, id_vars=['node', "nb_salarie"] + last_cols, value_vars=feat_cols, var_name='variable', value_name='value')
dfo = dfo.dropna()
out_filename = os.path.join("/dbfs" + result_path, 'results_tree.csv')
dfo.to_csv(out_filename, sep=";", decimal='.', index=False)

#######################
# EXPORTING GRAPHVIZ
#######################
out_filename = os.path.join("/dbfs" + result_path, 'results_graph.csv')
with open(out_filename, 'w') as out_file:
    out_file.write(tree.export_graphviz(clf.tree_,impurity=False, feature_names=list(features_to_use[1:]), out_file=None, rotate=True))

# COMMAND ----------

# DBTITLE 1,EXPORT CUBE
CUBE_VAR = VAR_FULL_TREE.alias('CUBE_VAR')

donnee_enfant_presente = len(get_col_unique(CUBE_VAR, "Enfants")) > 1
if not donnee_enfant_presente:
  CUBE_VAR = CUBE_VAR.withColumn("Enfants", F.lit("donnee manquante"))
  
donnee_presente = len(get_col_unique(CUBE_VAR, "Auxiliaire")) > 1
if not donnee_presente:
  CUBE_VAR = CUBE_VAR.withColumn("Auxiliaire", F.lit("donnee manquante"))
  
donnee_presente = len(get_col_unique(CUBE_VAR, "majorations_mois_precedent_eleve")) > 1
if not donnee_presente:
  CUBE_VAR = CUBE_VAR.withColumn("majorations_mois_precedent_eleve", F.lit("donnee manquante"))

donnee_presente = len(get_col_unique(CUBE_VAR, "prime_annee_glissante")) > 1
if not donnee_presente:
  CUBE_VAR = CUBE_VAR.withColumn("prime_annee_glissante", F.lit("donnee manquante"))
  
  
lmreq = CUBE_VAR.groupBy("mois_annee").agg(F.countDistinct("date_roulement").alias("nbd")).orderBy("mois_annee").filter(F.col("nbd")>=1).rdd
lm = lmreq.max()[0]
fm = lmreq.takeOrdered(3)
fm = [e[0] for e in fm]


renaming = {"lib_ligne": "Ligne", "lib_depot_add": "Depot", "duree_jours": "Duree_absence_jours", "lib_categorie_professionnelle": "Categorie_professionnelle", "lib_activite": "Activite", "type_voiture": "Type Vehicule"}

for k, v in renaming.items():
  CUBE_VAR = CUBE_VAR.withColumnRenamed(k, v)
CUBE_VAR = CUBE_VAR.withColumn("dernier_mois", F.when(F.col("mois_annee")==lm, 1).otherwise(0))
CUBE_VAR = CUBE_VAR.withColumn("Typologie_absence", F.col('TYPO_ABS_HISTO'))

period_filters = ["Jour_Ferie_2J", "Jour_Religieux_2J", "Periode_Grippe", "Periode_Gastro", "Periode_Epidemie", 
                          "Periode_vacances_scolaires", "Periode_neige", "Periode_temperatures_extremes", "Weekend", "Jour_ouvre", "Vendredi", "Lundi"]

CUBE_VAR = CUBE_VAR.withColumn("id_salarie", F.col("id_salarie").cast("string"))
CUBE_VAR = CUBE_VAR.withColumn("date_evt", F.col("date_roulement"))

CUBE_VAR_FICTIF = CUBE_VAR.select("date_roulement", "id_salarie", "date_evt")
CUBE_VAR_FICTIF = CUBE_VAR_FICTIF.withColumn("id_salarie", F.lit(None).cast("string"))
CUBE_VAR_FICTIF = CUBE_VAR_FICTIF.withColumn("date_evt", F.to_date(F.lit("1970-01-01")))

CUBE_VAR_FICTIF = CUBE_VAR_FICTIF.drop_duplicates()

def harmonize_schemas_and_combine(df_left, df_right, default_string="donnee manquante", default_number=0):
    left_types = {f.name: f.dataType for f in df_left.schema}
    right_types = {f.name: f.dataType for f in df_right.schema}
    left_fields = set((f.name, f.dataType, f.nullable) for f in df_left.schema)
    right_fields = set((f.name, f.dataType, f.nullable) for f in df_right.schema)

    # First go over left-unique fields
    for l_name, l_type, l_nullable in left_fields.difference(right_fields):
        if l_name in right_types:
            r_type = right_types[l_name]
            if l_type != r_type:
                raise TypeError("Union failed. Type conflict on field %s. left type %s, right type %s" % (l_name, l_type, r_type))
            else:
                raise TypeError("Union failed. Nullability conflict on field %s. left nullable %s, right nullable %s"  % (l_name, l_nullable, not(l_nullable)))
        if type(l_type) is StringType:
          default = default_string
        elif (type(l_type) is IntegerType) or (type(l_type) is DoubleType):
          default = default_number
        else:
          default = "1970-01-01"
        df_right = df_right.withColumn(l_name, F.lit(default).cast(l_type))

    # Now go over right-unique fields
    for r_name, r_type, r_nullable in right_fields.difference(left_fields):
        if r_name in left_types:
            l_type = left_types[r_name]
            if r_type != l_type:
                raise TypeError("Union failed. Type conflict on field %s. right type %s, left type %s" % (r_name, r_type, l_type))
            else:
                raise TypeError("Union failed. Nullability conflict on field %s. right nullable %s, left nullable %s" % (r_name, r_nullable, not(r_nullable)))
        if type(r_type) is StringType:
          default = default_string
        elif (type(r_type) is IntegerType) or (type(l_type) is DoubleType):
          default = default_number
        else:
          default = "1970-01-01"

        df_left = df_left.withColumn(r_name, F.lit(default).cast(r_type))    
      
    # Make sure columns are in the same order
    df_left = df_left.select(df_right.columns)

    return df_left.union(df_right)

CUBE_VAR = harmonize_schemas_and_combine(CUBE_VAR, CUBE_VAR_FICTIF)
CUBE_VAR = CUBE_VAR.withColumn("id_salarie", F.when(F.col("id_salarie").isNull(), "-1").otherwise(F.col("id_salarie")))
for c in period_filters:
  CUBE_VAR = CUBE_VAR.withColumn(c, F.when(F.col("id_salarie")=="-1", F.lit(1)).otherwise(F.col(c)))

CUBE_VAR = CUBE_VAR.withColumn("FLAG_ABS", F.when(F.col("id_salarie")=="-1", F.lit(0)).otherwise(F.col("FLAG_ABS")))
CUBE_VAR = CUBE_VAR.withColumn("heures_service", F.when(F.col("id_salarie")=="-1", F.lit(1)).otherwise(F.col("heures_service")))
CUBE_VAR = CUBE_VAR.withColumn("mois_annee", F.when(F.col("id_salarie")=="-1", F.lit(fm[0])).otherwise(F.col("mois_annee")))

CUBE_VAR = CUBE_VAR.withColumn("premiers_mois", F.when(F.col("mois_annee").isin(fm), 1).otherwise(0))

donneesm = ["Salaire", "Augmentation", "Contrat", "Type Vehicule"]
for c in donneesm : 
  CUBE_VAR = CUBE_VAR.withColumn(c, F.when(F.col(c).isNull(), "donnee manquante").otherwise(F.col(c)))


CUBE_VAR = CUBE_VAR.select('id_salarie', 'date_roulement', "date_evt", "FLAG_ABS", "service_agent_id", "TYPE_ABS", "premiers_mois",
                           "Categorie_professionnelle", "Activite", 
                          "Jour_Ferie_2J", "Jour_Religieux_2J", "Periode_Grippe", "Periode_Gastro", "Periode_Epidemie", 
                          "Periode_vacances_scolaires", "Periode_neige", "Periode_temperatures_extremes", "Weekend", "Jour_ouvre", "Vendredi", "Lundi",
                          'effectif_reel', 'TYPO_ABS_HISTO',
                          'Horaires', 'Amplitudes horaires', 'Service soiree', 
                          'Ligne', 'Depot', "Type Vehicule", "id_depot", "lib_depot",
                          'Sexe', 'Situation_familiale', 'nombre_enfants',
                          'Anciennete', 'Contrat', 'Temps_partiel',
                          'Enfants', 'Tranche_Age', 
                          'Salaire', 'Augmentation', "majorations_mois_precedent_eleve", "prime_annee_glissante", "prime_histo", "Auxiliaire",
                          "Temps_trajet_dom_trav",
                          'Bcp_accidents_corporels_3J','Bcp_accidents_materiels_3J', 'Bcp_accidents_3J',
                          'node', 'Duree_absence_jours', 'type_duree', "heures_service",
                          "FREQUENCE_ABS_HISTO_MOIS", "DUREE_ABS_HISTO_MOIS", 'ENTREE_OU_SORTIE')

export_result_df(CUBE_VAR, 'variables_cube.csv')


# COMMAND ----------



# COMMAND ----------

SUM_VAR = VAR_FULL_TREE

SUM_VAR = SUM_VAR.withColumn("HS_ABS", F.when(F.col("FLAG_ABS")==1, F.col('heures_service')).otherwise(0))\
                 .withColumn("HS_PRES", F.when(F.col("FLAG_ABS")==0, F.col('heures_service')).otherwise(0))\
                 .withColumn("YEAR", F.year(F.col("date_roulement")))\
                 .withColumn("MONTH", F.month(F.col("date_roulement")))
# display(SUM_VAR.groupBy("TYPE_ABS").count())
ret = []
for k, tlist in {"MAL": ["MALADIE"], "AT": ["ACCIDENT"], "TOUT": ["ACCIDENT", "MALADIE"]}.items():
  SUM_VAR_T = SUM_VAR.filter((F.col("FLAG_ABS")==0) | (F.col("TYPE_ABS").isin(tlist)))\
                     .groupBy("YEAR", "MONTH").agg(F.sum("HS_ABS").alias('HS_ABS'), F.sum("HS_PRES").alias('HS_PRES'))\
                     .withColumn("HS_TOT", F.col("HS_ABS")+F.col("HS_PRES")).drop("HS_PRES")\
                     .withColumn("TYPE", F.lit(k))
  ret += [SUM_VAR_T]
  
SUM_VAR_2 = ret[0].union(ret[1]).union(ret[2])
win_spec = Window.partitionBy("TYPE").orderBy('YEAR', 'MONTH').rowsBetween(-11, 0)
win_spec2 = Window.partitionBy("TYPE", "YEAR").orderBy('YEAR', 'MONTH').rowsBetween(-sys.maxsize, 0)

SUM_VAR_2 = SUM_VAR_2.withColumn('CUM_HS_ABS', F.sum("HS_ABS").over(win_spec))\
                     .withColumn('CUM_HS_TOT', F.sum("HS_TOT").over(win_spec))\
                     .withColumn('CUM_HS_ABS_YTD', F.sum("HS_ABS").over(win_spec2))\
                     .withColumn('CUM_HS_TOT_YTD', F.sum("HS_TOT").over(win_spec2))\
                     .withColumn('Absenteisme 12 mois glissants', F.col('CUM_HS_ABS') / F.col('CUM_HS_TOT'))\
                     .withColumn('Absenteisme depuis janvier', F.col('CUM_HS_ABS_YTD') / F.col('CUM_HS_TOT_YTD'))\
                     .withColumn('Absenteisme mensuel', F.col('HS_ABS') / F.col('HS_TOT'))\
#                  .drop("HS_ABS", "HS_TOT", "CUM_HS_ABS", "CUM_HS_TOT")

# display(SUM_VAR_2)

ret = []
for y in get_col_unique(SUM_VAR_2, "YEAR"):
  for c in ['Absenteisme 12 mois glissants', 'Absenteisme depuis janvier', 'Absenteisme mensuel']:
    print(c, y)
    SUM_VAR_Y = SUM_VAR_2.filter(F.col("YEAR")==y)\
                         .withColumnRenamed(c, "VALUE")\
                         .withColumn("VARIABLE",  F.lit("{} {} ".format(c, y)))\
                          .select("YEAR", "MONTH","TYPE","VALUE","VARIABLE")
    ret += [SUM_VAR_Y]

print(ret)

SUM_VAR_OUT = ret.pop(0)

while len(ret) > 0:
  print(ret)
  r = ret.pop(0)
  SUM_VAR_OUT = SUM_VAR_OUT.union(r)
  
  
export_result_df(SUM_VAR_OUT, 'results_month.csv')

# display(SUM_VAR_OUT)

# COMMAND ----------


