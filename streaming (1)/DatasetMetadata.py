# Databricks notebook source
# test urls for testing
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job/snapshot/piscdb.cisadm_job.snapshot.20220310.parquet
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/metadataazurefunction/fakedataset1truncate/truncate_load/metadataazurefunction.fakedataset1truncate.truncate_load.20220225045041.parquet
#https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job_advertisement/snapshot/piscdb.cisadm_job_advertisement.snapshot.20220310.parquet

# COMMAND ----------

# manually provide the configurations
# format for these urls is "https://{storage account name}.blob.core.windows.net/{container name}/{rest of the path}"
# format for the id is "{source name}__{dataset name}"
dataset_configurations = [
    {
        "dataset_id": "piscdb__cisadm_job",
        "ingest_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job/snapshot",
        "delta_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/delta/piscdb/cisadm_job"
    },
    {
        "dataset_id": "piscdb__cisadm_job_advertisement",
        "ingest_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/piscdb/cisadm_job_advertisement/snapshot",
        "delta_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/delta/piscdb/cisadm_job_advertisement"
    },
    {
        "dataset_id": "tests__test_line_delimited_json",
        "ingest_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/tests/test_line_delimited_json/snapshot",
        "delta_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/delta/tests/test_line_delimited_json"
    },
    {
        "dataset_id": "tests__test_csv",
        "ingest_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/ingest/tests/test_csv/snapshot",
        "delta_root": "https://gdpcorerefimplrawsagen2.blob.core.windows.net/delta/tests/test_csv"
    }
]

# COMMAND ----------

# to keep the amount of manual specification low in the above metadata, we need to add some additional
# metadata parsed from the above. This also does some validation of the configurations.

from urllib.parse import urlparse
def enhance_dataset(meta):
    
    url = urlparse(meta['ingest_root'])
    meta['ingest_storage_account_name'] = url.netloc.split(".")[0]
    meta['ingest_storage_account_container_name'] = url.path.split("/")[1]
    meta['ingest_path'] = url.path[len(meta['ingest_storage_account_container_name'])+1:]
    meta['ingest_mount_root'] = f"/mnt/{meta['ingest_storage_account_name']}/{meta['ingest_storage_account_container_name']}" 
    meta['ingest_mount_path'] = f"{meta['ingest_mount_root']}{meta['ingest_path']}" 
    
    url = urlparse(meta['delta_root'])
    meta['delta_storage_account_name'] = url.netloc.split(".")[0]
    meta['delta_storage_account_container_name'] = url.path.split("/")[1]
    meta['delta_path'] = url.path[len(meta['delta_storage_account_container_name'])+1:]
    meta['delta_mount_root'] = f"/mnt/{meta['delta_storage_account_name']}/{meta['delta_storage_account_container_name']}" 
    meta['delta_mount_path'] = f"{meta['delta_mount_root']}{meta['delta_path']}" 
    
    meta['loads_delta_files_path'] = f"/mnt/{meta['delta_storage_account_name']}/logs/{meta['delta_storage_account_name']}__loads/"
    meta['loads_delta_table_name'] = f"loads.{meta['delta_storage_account_name']}__loads"

    meta['source_name'] = meta['dataset_id'].split('__')[0]
    meta['dataset_name'] = meta['dataset_id'][len(meta['source_name'])+2:]
    
    return meta
    
#print(parse_dataset_roots(dataset_configurations[0]))

# COMMAND ----------

def do_files_exist_on_path(path):
    try:
        return any(dbutils.fs.ls(path))
    except Exception as ex:
        return False

# COMMAND ----------

from delta.tables import *
from pyspark.sql import Row

# Enable automatic schema evolution which will be needed below
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

def write_load(load):
    
    df = spark.createDataFrame(Row(**x) for x in list([load]))
    
    if not do_files_exist_on_path(params['loads_delta_files_path']):

        # if the table does not exist then we are creating it
        df.write.format("delta") \
          .save(params['loads_delta_files_path'])
    
    else:
        
        delta_table = DeltaTable.forPath(spark, load['loads_delta_files_path'])

        delta_table.alias('tbl') \
              .merge(df.alias('updates'), 'tbl.load_id = updates.load_id') \
              .whenMatchedUpdateAll() \
              .whenNotMatchedInsertAll() \
              .execute()
    
    spark.sql(f"""
    CREATE DATABASE if not exists loads;
    """)

    spark.sql(f"""
    CREATE TABLE if not exists {params["loads_delta_table_name"]}
        USING DELTA
        LOCATION '{params["loads_delta_files_path"]}'
        """)
    
    spark.sql(f"""
    REFRESH TABLE {params["loads_delta_table_name"]}
        """)
    

# COMMAND ----------

from delta.tables import *
from pyspark.sql import Row

# Enable automatic schema evolution which will be needed below
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 



def write_scan(scan):
    
    df = spark.createDataFrame(Row(**x) for x in list([scan]))
    
    if not do_files_exist_on_path(params['scans_delta_files_path']):

        # if the table does not exist then we are creating it
        df.write.format("delta") \
          .save(params['scans_delta_files_path'])
    
    else:
        
        delta_table = DeltaTable.forPath(spark, scan['scans_delta_files_path'])

        delta_table.alias('tbl') \
              .merge(df.alias('updates'), 'tbl.scan_id = updates.scan_id') \
              .whenMatchedUpdateAll() \
              .whenNotMatchedInsertAll() \
              .execute()
    
    spark.sql(f"""
    CREATE DATABASE if not exists scans;
    """)

    spark.sql(f"""
    CREATE TABLE if not exists {params["scans_delta_table_name"]}
        USING DELTA
        LOCATION '{params["scans_delta_files_path"]}'
        """)
    
    spark.sql(f"""
    REFRESH TABLE {params["scans_delta_table_name"]}
        """)
    

# COMMAND ----------

import json
def get_metadata_from_ingest_file_url(ingest_file_url):
   
    # first try to resolve it from the file name
    file_url = urlparse(ingest_file_url)
    source_name = file_url.path.split("/")[-1].split(".")[0]
    dataset_name = file_url.path.split("/")[-1].split(".")[1]
    result = [_ for _ in dataset_configurations if _['dataset_id'] == f"{source_name}__{dataset_name}"]
    if len(result) == 1:
        return enhance_dataset(result[0])
    
    # next query all the datasets and try to match the ingest file url path
    def match_url_root(url, root_url):
        return url[:len(root_url)] == root_url
    result = [_ for _ in dataset_configurations if match_url_root(ingest_file_url, _['ingest_root'])]
    if len(result) == 1:
        return enhance_dataset(result[0])
    
    return None

# COMMAND ----------

def get_metadata_by_dataset_id(dataset_id):
    dataset = [_ for _ in dataset_configurations if _['dataset_id'] == dataset_id]
    if len(dataset) == 0 or len(dataset) > 1:
        return None
    
    return enhance_dataset(dataset[0])

def get_all_metadata():
    return [enhance_dataset(_) for _ in dataset_configurations]

# COMMAND ----------


