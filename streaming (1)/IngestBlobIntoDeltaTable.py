# Databricks notebook source
# test urls for testing
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job/snapshot/piscdb.cisadm_job.snapshot.20220310.parquet
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/metadataazurefunction/fakedataset1truncate/truncate_load/metadataazurefunction.fakedataset1truncate.truncate_load.20220225045041.parquet
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job_advertisement/snapshot/piscdb.cisadm_job_advertisement.snapshot.20220310.parquet

# COMMAND ----------

# MAGIC %pip install azure-storage-blob python-slugify flexidate JayDeBeApi

# COMMAND ----------

# MAGIC %run ./DatasetMetadata

# COMMAND ----------

from urllib.parse import urlparse
from slugify import slugify
from pyspark.sql.functions import *#sha2, concat_ws, current_timestamp
import base64
from pyspark.sql.functions import col, lit, unix_timestamp, to_timestamp
from datetime import datetime, timedelta
from pyspark.sql import Row
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
import json
import uuid
import requests
from flexidate import parse
def b64(o): return base64.b64encode(o.encode('utf-8')).decode("utf-8")
to_base64 = udf(lambda z: b64(z))
def tsStr(o) : return str(o)

def bail(ex=None, step_name=None):
  params['error'] = str(ex)
  params['last_error_step_name'] = step_name
  params['load_status'] = 'error'
  params['load_end'] = datetime.utcnow().isoformat() + 'Z'
  write_load(params)
  if ex:
    print(ex, params)
    raise ex
  else:
    dbutils.notebook.exit(json.dumps(params))

dbutils.widgets.text("ingest_file_mount_path", "/mnt/gdpcorerefimplrawsagen2/ingest/piscdb/cisadm_job/snapshot/piscdb.cisadm_job.snapshot.20220310.parquet")
dbutils.widgets.text("dataset_id", "piscdb__cisadm_job")
dbutils.widgets.text("ingest_file_timestamp", "2022-10-03T00:00:00Z")


params = {}
params['load_id'] = str(uuid.uuid4())
params['load_start'] = datetime.utcnow().isoformat() + 'Z'
params['ingest_file_mount_path'] = dbutils.widgets.get("ingest_file_mount_path")
params['dataset_id'] = dbutils.widgets.get("dataset_id")
params['ingest_file_timestamp'] = dbutils.widgets.get("ingest_file_timestamp")


try:

    metadata = get_metadata_by_dataset_id(params['dataset_id'])

    if not metadata:
        raise Exception(f"metadata not found for `{dbutils.widgets.get('ingest_file_mount_path')}`")

    params.update(metadata)
    
    # define and extract the main parameters for the notebook
    params['ingest_file_extension'] = urlparse(params['ingest_file_mount_path']).path.split(".")[-1].lower()
    
    
    params['ingest_file_treatment'] = urlparse(params['ingest_file_mount_path']).path.split("/")[-1].split(".")[-3]
    if params['ingest_file_treatment'] == "truncate_load":
        params['spark_write_mode'] = 'overwrite'
    elif params['ingest_file_treatment'] == "append":
        params['spark_write_mode'] = 'merge'
    elif params['ingest_file_treatment'] == "delta":
        params['spark_write_mode'] = 'merge'
    elif params['ingest_file_treatment'] == "full":
        params['spark_write_mode'] = 'merge'
    elif params['ingest_file_treatment'] == "reload":
        params['spark_write_mode'] = 'overwrite'
    elif params['ingest_file_treatment'] == "snapshot":
        params['spark_write_mode'] = 'merge'
    elif params['ingest_file_treatment'] == "overwrite":
        params['spark_write_mode'] = 'overwrite'
    else:
        bail("ingest_file_treatment not recognized", "config")

    params['spark_overwrite_schema'] = "true" if params['spark_write_mode'] == 'overwrite' else 'false'

except Exception as ex:
    bail(ex, "config")

    
if not any(mount.mountPoint == params['ingest_mount_root'] for mount in dbutils.fs.mounts()):
    bail("Ingest storage account + container is not mounted", "config")

if not any(mount.mountPoint == params['delta_mount_root'] for mount in dbutils.fs.mounts()):
    bail("Delta storage account + container is not mounted", "config")

    
# check the blob to see if it exists
try:
    params['ingest_file_size'] = dbutils.fs.ls(params['ingest_file_mount_path'])[0].size
except Exception as ex:
    bail(ex, 'config')

    
# check to see if the destination table exists
try:
    params['delta_table_exists'] = any(dbutils.fs.ls(params['delta_mount_path']))
except Exception as ex:
    params['delta_table_exists'] = False

write_load(params)
    
params

# COMMAND ----------

try:

    # read the ingestion file
    params['read_start'] = datetime.utcnow().isoformat() + 'Z'

    if params['ingest_file_extension'] == "jsonl":
        ingest_file = spark.read.json(params['ingest_file_mount_path'])
    elif params['ingest_file_extension'] == "parquet":
        ingest_file = spark.read.parquet(params['ingest_file_mount_path'])
    elif params['ingest_file_extension'] == "csv":
        ingest_file = spark.read.csv(params['ingest_file_mount_path'], header=True, inferSchema=False)
    else:
        raise Exception(f"ingest file extension `{params['ingest_file_extension']}` not recognized")
        
    params['ingest_file_row_count'] = ingest_file.count()
    params['read_end'] = datetime.utcnow().isoformat() + 'Z'

except Exception as ex: 
    bail(ex, 'read') # if there is a failure here then we should not continue

ingest_file.printSchema()
ingest_file.show()

# COMMAND ----------

try:

    params['prepare_start'] = datetime.utcnow().isoformat() + 'Z'

    # save the original schema before we change anything
    params['ingest_file_schema_original'] = "|".join([f"{_['name']} {_['type']}" for _ in json.loads(ingest_file.schema.json())['fields']])
    
    # rename the columns to something consistent with the SQL column name constraints
    for column in ingest_file.columns:
        ingest_file = ingest_file.withColumnRenamed(column, slugify(column, separator="_"))

    # prepare the list of columns to be hashed, converting to json string if not a simple data type
    sorted_columns = sorted(json.loads(ingest_file.schema.json())['fields'], key=lambda k:k['name'])
    cols = [col(_['name']) if not isinstance(_['type'], dict) else to_json(col(_['name'])) for _ in sorted_columns]

    # add a column with a 256-bit SHA2 hash key of all other columns
    ingest_file = ingest_file.withColumn("dl_hash_key", sha2(concat_ws("||", *cols), 256))

    # add a default partition column derived from the hash key
    ingest_file = ingest_file.withColumn("dl_partition_key", ingest_file['dl_hash_key'].substr(1, 1))

    # add a column with a link to the load metadata
    ingest_file = ingest_file.withColumn("dl_first_load_id", lit(params['load_id']))
    ingest_file = ingest_file.withColumn("dl_last_load_id", lit(params['load_id']))

    # record the final schema
    params['ingest_file_schema_final'] = "|".join([f"{_['name']} {_['type']}" for _ in json.loads(ingest_file.schema.json())['fields']])

    params['prepare_end'] = datetime.utcnow().isoformat() + 'Z'

except Exception as ex: 
    bail(ex, 'prepare') # if there is a failure here then we should not continue

ingest_file.printSchema()
#ingest_file.show()
params

# COMMAND ----------

# MAGIC %md
# MAGIC this is only neccessary if the partition changes
# MAGIC     dbutils.fs.rm(params['destination_delta_files_path'] + "_delta_log/__tmp_path_dir/" ,recurse=True)
# MAGIC     dbutils.fs.rm(params['destination_delta_files_path'] + "_delta_log/" ,recurse=True)
# MAGIC     dbutils.fs.rm(params['destination_delta_files_path'],recurse=True)`

# COMMAND ----------

from delta.tables import *

# manually setting this here for testing so it always processes
params['spark_write_mode'] == 'merge'

try:

    # write the new records to the delta table
    params['update_start'] = datetime.utcnow().isoformat() + 'Z'
    
    # create a delta table object if the table exists
    delta_table = None
    if params['delta_table_exists']:
        delta_table = DeltaTable.forPath(spark, params['delta_mount_path'])
    
        # record the schema
        params['delta_schema_original'] = ('|'.join(sorted([field.simpleString() for field in delta_table.toDF().schema]))).replace(':', ' ')
    
    if not params['delta_table_exists']:
        
        # if the table does not exist then we are creating it
        params['update_operation'] = 'create'
        ingest_file.write.format("delta") \
          .partitionBy("dl_partition_key") \
          .save(params['delta_mount_path'])
        
    elif params['spark_write_mode'] == 'overwrite':

        # if the spark mode is overwrite then we assume we are just overwriting all the existing data
        params['update_operation'] = 'overwrite'
        ingest_file.write.format("delta") \
          .mode('overwrite') \
          .option('overwriteSchema', True) \
          .partitionBy("dl_partition_key") \
          .save(params['delta_mount_path'])
        
    elif params['spark_write_mode'] == 'merge':

        # otherwise do a merge operation on the table and the new data
        params['update_operation'] = 'merge'
        
        # note that there's no need to update the partition key since its always the same for a given hash
        delta_table.alias('tbl') \
          .merge(ingest_file.alias('updates'), 'tbl.dl_hash_key = updates.dl_hash_key and tbl.dl_partition_key = updates.dl_partition_key') \
          .whenMatchedUpdate(set = { "dl_last_load_id": "updates.dl_last_load_id" } ) \
          .whenNotMatchedInsertAll() \
          .execute()
        
        history = spark.sql(f"DESCRIBE HISTORY '{params['delta_mount_path']}' LIMIT 1").toPandas().to_dict("records")[0]
        params['write_rows_inserted'] = history.get("operationMetrics", {}).get("numTargetRowsInserted")
        params['write_rows_updated'] = history.get("operationMetrics", {}).get("numTargetRowsUpdated")
       
        
    # refresh the metadata and record the final schema
    delta_table = DeltaTable.forPath(spark, params['delta_mount_path'])
    params['delta_schema_final'] = ('|'.join(sorted([field.simpleString() for field in delta_table.toDF().schema]))).replace(':', ' ')

    params['update_end'] = datetime.utcnow().isoformat() + 'Z'
    
except Exception as ex: 
    print(ex)
    bail(ex, 'update') # if there is a failure here then we should not continue

    
ingest_file.printSchema()
#ingest_file.show()
params

# COMMAND ----------

params['load_status'] = 'success'
params['load_end'] = datetime.utcnow().isoformat() + 'Z'
write_load(params)
dbutils.notebook.exit(json.dumps(params))

# COMMAND ----------

"update_start": "2022-03-11T22:13:53.611711Z", "update_operation": "overwrite", "update_end": "2022-03-11T22:15:36.007060Z"
"update_start": "2022-03-11T22:16:19.072651Z", "update_operation": "merge", "update_end": "2022-03-11T22:17:30.731130Z"

# COMMAND ----------

# MAGIC %md
# MAGIC This is the code to update the synapse workspace

# COMMAND ----------


    
    """
    file_url = urlparse(dbutils.widgets.get("ingest_file_url"))
    params['ingest_file_url'] = dbutils.widgets.get("ingest_file_url")
    file_url = urlparse(params["ingest_file_url"])
    params['ingest_file_mount_path'] = f"/mnt/{params['ingest_storage_account_name']}{file_url.path.split('?')[0]}" 
    
    
    params['logs_delta_files_path'] = f"/mnt/{params['delta_storage_account_name']}/logs/{params['delta_storage_account_name']}__loads/"
    params['logs_delta_table_name'] = f"logs.{params['delta_storage_account_name']}__loads"
  
    # define and extract the main parameters for the notebook
    file_url = urlparse(dbutils.widgets.get("ingest_file_url"))
    params['ingest_file_url'] = dbutils.widgets.get("ingest_file_url")
    file_url = urlparse(params["ingest_file_url"])
    params['ingest_file_mount_path'] = f"/mnt/{params['ingest_storage_account_name']}/{params['ingest_storage_account_container_name']}" 
    params['ingest_file_blob_path'] = f"/mnt/{params['ingest_storage_account_name']}{file_url.path.split('?')[0]}" 
    params['ingest_file_extension'] = file_url.path.split(".")[-1].lower()
    params['ingest_files_path'] = f"/mnt/{params['ingest_storage_account_name']}/{params['ingest_storage_account_container_name']}{params['ingest_path']}" 
    
    # todo base this on the delta path from the dataset
    params['destination_delta_files_path'] = f"/mnt/{params['delta_storage_account_name']}/{params['delta_storage_account_container_name']}{params['delta_path']}"
    params['destination_delta_local_files_path'] = params['delta_path']
    params['delta_mount_path'] = f"/mnt/{params['delta_storage_account_name']}/{params['delta_storage_account_container_name']}" 
    
    """

# COMMAND ----------

# for some: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/develop-openrowset#type-mapping-for-parquet
def map_parquet_type_sql_server_type(parquet_type):
    if "byte_array" in parquet_type:
        return "varbinary(8000)"
    if "timestamp" in parquet_type:
        return "datetime2"
    if "binary" in parquet_type:
        return "varbinary(8000)"
    if "decimal" in parquet_type:
        return "decimal"
    if "string" in parquet_type:
        return "varchar(max)"
    raise Exception(f"parquet type `{parquet_type}` not recognized")
    
# run the final schema through the mapping to ensure that it won't fail later
schema = params['destination_delta_schema_final'].split("|")
[(_.split(" ")[0], _.split(" ")[1], map_parquet_type_sql_server_type(_.split(" ")[1])) for _ in schema]

# COMMAND ----------

database_name = "audit"
admin_password = "fsyrfgrrrr1#1"
fully_qualified_table_name = f"{database_name}.{params['source_name']}.{params['dataset_name']}"
schema_name = params['source_name']
table_name = params['dataset_name']
sa_name = "gdpcorerefimplbronzesag2"
container_name = "bronze"
credential_name = f"{database_name}__{params['source_name']}__{params['dataset_name']}__credential"
file_format_name = f"{database_name}__{params['source_name']}__{params['dataset_name']}__format"
data_source_name = f"{database_name}__{params['source_name']}__{params['dataset_name']}__data_source"
sas_token = "?sv=2020-08-04&ss=bf&srt=sco&sp=rl&se=2023-03-26T12:53:34Z&st=2022-03-26T04:53:34Z&spr=https&sig=q%2Bh%2FL7585KhV6Ccnj7aCFG3DGESh%2BH%2F9bGLL5Lvb7l4%3D"
file_location_path = params['destination_delta_local_files_path'] + "*/*.parquet"

# COMMAND ----------


# make sure there is a master encryption key in the database
import jaydebeapi
con = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                           "jdbc:sqlserver://gdpcorerefimplaudit-ondemand.sql.azuresynapse.net",
                           ["sqladminuser", admin_password])

# create the database if it doesn't exist
with con.cursor() as cursor:
    print('checking database', database_name)
    cursor.execute(f"SELECT * FROM sys.databases WHERE name = '{database_name}'")
    if not cursor.fetchone():
        print('creating database', database_name)
        cursor.execute(f"CREATE DATABASE {database_name}")

with con.cursor() as cursor:
    cursor.execute(f"USE {database_name}")
    print('checking master encryption key')
    cursor.execute(f"SELECT * FROM sys.databases WHERE name = '{database_name}' and is_master_key_encrypted_by_server = 1")
    if not cursor.fetchone():
        print('creating master encryption key')
        cursor.execute(f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{admin_password}'")

# create the schema if it doesn't exist
with con.cursor() as cursor:
    cursor.execute(f"USE {database_name}")
    print('checking schema', schema_name)
    cursor.execute(f"SELECT * FROM {database_name}.sys.schemas WHERE name = '{schema_name}'")
    if not cursor.fetchone():
        print('creating schema', schema_name)
        cursor.execute(f"CREATE schema {schema_name}")
        
# check if the table already exists and unwind it if it does
# the order here is important as its reversed from the dependnecy chain
with con.cursor() as cursor:
    print('checking table', fully_qualified_table_name)
    cursor.execute(f"USE {database_name}")
    cursor.execute(f"SELECT name FROM {database_name}.sys.external_tables WHERE name = '{table_name}' AND schema_id = (SELECT schema_id FROM {database_name}.sys.schemas WHERE name = '{schema_name}')")
    if cursor.fetchone():
        print('unwinding table', fully_qualified_table_name)
        cursor.execute(f"DROP EXTERNAL TABLE {fully_qualified_table_name};")
        cursor.execute(f"DROP EXTERNAL FILE FORMAT {file_format_name};")
        cursor.execute(f"DROP EXTERNAL DATA SOURCE {data_source_name};")

# create the storage account credential, dropping first if neccessary
with con.cursor() as cursor:
    print('checking storage account credential', credential_name)
    cursor.execute(f"USE {database_name}")
    cursor.execute(f"SELECT name FROM {database_name}.sys.database_scoped_credentials WHERE name = '{credential_name}'")
    if cursor.fetchone():
        print('dropping storage account credential', credential_name)
        cursor.execute(f"DROP DATABASE SCOPED CREDENTIAL {credential_name};")
    print('creating storage account credential', credential_name)
    cursor.execute(f"CREATE DATABASE SCOPED CREDENTIAL {credential_name} WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = '{sas_token}';")

# create the external table
with con.cursor() as cursor:
    cursor.execute(f"USE {database_name}")

    # create the data source
    print('creating data source', data_source_name)
    cursor.execute(f"""
        CREATE EXTERNAL DATA SOURCE {data_source_name} WITH (
            LOCATION = 'https://{sa_name}.blob.core.windows.net/{container_name}',
            CREDENTIAL = {credential_name}
        );
    """)

    # create the file format
    print('creating file format', file_format_name)
    cursor.execute(f"""
        CREATE EXTERNAL FILE FORMAT {file_format_name}
        WITH (  
            FORMAT_TYPE = PARQUET
        );""")

    # finally the external table itself
    schema = ",".join([_.split(" ")[0] + ' ' + map_parquet_type_sql_server_type(_.split(" ")[1]) for _ in params['destination_delta_schema_final'].split("|")])
    print('creating table', fully_qualified_table_name, schema)
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {fully_qualified_table_name}
        ({schema})
        WITH (
            LOCATION = '{file_location_path}',
            DATA_SOURCE = {data_source_name},
            FILE_FORMAT = {file_format_name}
        )
    """)
    
with con.cursor() as cursor:
    cursor.execute(f"select top 1 * from {fully_qualified_table_name}")
    print(cursor.fetchall())
