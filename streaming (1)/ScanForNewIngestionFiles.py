# Databricks notebook source
# MAGIC %pip install flexidate

# COMMAND ----------

# MAGIC %run ./DatasetMetadata

# COMMAND ----------

from datetime import datetime
from urllib.parse import urlparse
from flexidate import parse
import uuid
import json

params = {}
params['scan_id'] = str(uuid.uuid4())
params['scan_start'] = datetime.utcnow().isoformat() + 'Z'
params['scans_delta_files_path'] = f"/mnt/gdpcorerefimplrawsagen2/logs/gdpcorerefimplrawsagen2__scans/"
params['scans_delta_table_name'] = f"scans.gdpcorerefimplrawsagen2__scans"
    
# recrusively list the files in a path
def lsr(path):
    return([fname for flist in [([fi.path] if fi.isFile() else lsR(fi.path)) for fi in dbutils.fs.ls(path)] for fname in flist])

# determine if a blob has already been ingested
def is_ingested(ingest_file_mount_path, loads):
    return len([_ for _ in loads if _['ingest_file_mount_path'] == ingest_file_mount_path and params.get('load_status') == 'success']) > 0
 
# function to extract the timestamp from a mounted blob path    
def get_timestamp(ingest_file_mount_path):
    try:
        #expects to see a file blob name like source_name.dataset_name.ingestion_treatment.YYYYMMDDTHHMMSSZ.parquet
        return parse(urlparse(ingest_file_mount_path).path.split("/")[-1].split(".")[-2]).as_datetime().isoformat() + 'Z'
    except Exception as ex:
        # this doesn't work need to fix it
        modification_time = dbutils.fs.ls(ingest_file_mount_path)[0].modificationTime
        return parse(str(modification_time / 1000)).as_datetime().isoformat() + 'Z'
       
        
ingestion_jobs = []

for meta in get_all_metadata():
    # ingest_mount_path = f"/mnt/{meta['ingest_storage_account_name']}/{meta['ingest_storage_account_container_name']}{meta['ingest_path']}" 
    print(meta['ingest_mount_path'])
    
    # get a list of all the files in the path
    ingest_file_blob_paths = [_.split(':')[1] for _ in lsr(meta['ingest_mount_path'])]
    
    # get a list of all loads that have come from this path
    loads = spark.sql(f"select * from {meta['loads_delta_table_name']} where ingest_mount_path = '{meta['ingest_mount_path']}'") \
        .toPandas() \
        .to_dict("records")
    
    # filter down to files that should now be ingested
    to_ingest = [_ for _ in ingest_file_blob_paths if not is_ingested(_, loads)]
    
    for _ in to_ingest:
        ingestion_jobs.append({
            "ingest_file_timestamp" : get_timestamp(_), 
            "ingest_file_mount_path" : _, 
            "dataset_id": meta['dataset_id']}
        )

# save the jobs as a serialized json string in the parameters
params['scan_jobs'] = json.dumps(ingestion_jobs)

# write the initial scan parameters
write_scan(params)

ingestion_jobs

# COMMAND ----------

ingestion_jobs = [ingestion_jobs[2]]

# execute all the jobs sequentially
for job in ingestion_jobs:
    try:
        job['start'] = datetime.utcnow().isoformat() + 'Z'
        #job['result'] = json.loads(dbutils.notebook.run('./IngestBlobIntoDeltaTable', 60*60*60*4, job))
        job['end'] = datetime.utcnow().isoformat() + 'Z'
        params['scan_successes'] = params.get('scan_successes', 0) + 1
    except Exception as ex:
        job['result'] = {"result" : "exception", "value" : str(ex) }
        job['end'] = datetime.utcnow().isoformat() + 'Z'
        params['scan_fails'] = params.get('scan_fails', 0) + 1
        params['last_scan_fail'] = str(ex)
        print(ex)

# save the jobs as a serialized json string in the parameters
params['scan_jobs'] = json.dumps(ingestion_jobs)

params

# COMMAND ----------

   
params['scan_status'] = 'complete'
params['scan_end'] = datetime.utcnow().isoformat() + 'Z'
write_scan(params)
dbutils.notebook.exit(json.dumps(params))

# COMMAND ----------

# MAGIC %md
# MAGIC Below code explores the eventual multi-threading solution

# COMMAND ----------

import asyncio
import http3
parallel = 1

# configuration
root = 'https://dwdatalakesecureint.blob.core.windows.net/'
token = 'sv=2020-08-04&ss=bfqt&srt=sco&sp=ltf&se=2022-04-15T01:14:02Z&st=2022-04-13T17:14:02Z&spr=https&sig=pUbZ7O2SBlmwng7KRFQ9%2B0iOUs%2FKt1FSE6P56Nho99E%3D'
container_name = 'raw'
crawler_count = 10
root_prefix = 'secure/azuresqlservergrowtogetherlinkedservice/delta/dbo_v_leadminerusagedata' # '' use empty string for root/all
root_prefix = '' # '' use empty string for root/all

# native packages
import xml.etree.ElementTree as ET
import uuid
from datetime import datetime
import os
import sys
import csv
if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)

async def ingest_blobs(name, container_name, queue, result_queue):
    slept = False
    while not queue.empty() or not slept:
        if queue.empty() and not slept:
            await asyncio.sleep(5)
            slept = True
        else:
            slept = False
            dbutils.notebook.run("./IngestBlobIntoDeltaTable", 60*60*60, await queue.get())
            

async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)
    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(task) for task in tasks))

async def main():
    start_ts = datetime.now().timestamp()

    blob_queue = asyncio.Queue()
    for _ in ingestion_jobs:
        blob_queue
        
        todo figure out concurrency
    
    result_queue = asyncio.Queue()

    await prefix_queue.put(root_prefix)

    tasks = []
    for x in range(0, crawler_count):
        tasks.append(asyncio.ensure_future(crawl_prefixes(f'crawler_{x}', container_name, prefix_queue, result_queue)))

    await gather_with_concurrency(crawler_count, *tasks)

    blob_url_queue = asyncio.Queue()

    prefixes = []
    while not result_queue.empty():
        prefixes.append({'prefix': await result_queue.get(), 'next_marker': None})
        await blob_url_queue.put(prefixes[-1])

    tasks = []
    for x in range(0, crawler_count):
        tasks.append(asyncio.ensure_future(crawl_blobs(f'crawler_{x}', container_name, blob_url_queue, result_queue)))

    await gather_with_concurrency(crawler_count, *tasks)

    blobs = []
    while not result_queue.empty():
        blobs.append(await result_queue.get())

    print('crawlers:', crawler_count, 'terminal folders:', len(prefixes), 'blobs:', len(blobs), 'duration ex sleep:', datetime.now().timestamp() - start_ts - 10, 's')

    keys = blobs[0].keys()

    with open('./blobs.csv', 'w', newline="\n") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(blobs)


if __name__ == '__main__':
  
    asyncio.run(main())


# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
    self.path = path
    self.timeout = timeout
    self.parameters = parameters
    self.retry = retry

  def submitNotebook(notebook):
    print("Running notebook %s" % notebook.path, notebook.parameters)
    try:
      if (notebook.parameters):
        return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
      else:
        return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception as ex:
      print('EXCEPTION', notebook.parameters, ex)
      if notebook.retry < 1:
        return
    print("Retrying notebook %s" % notebook.path)
    notebook.retry = notebook.retry - 1
    submitNotebook(notebook)

def parallelNotebooks(notebooks, numInParallel):
   # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
   # This code limits the number of parallel notebooks.
   with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    return [ec.submit(NotebookData.submitNotebook, notebook) for notebook in notebooks]

  
def execute_pipelines(pipelines, concurrent = 1): #change here to multitennant the process\run multiple pipelines
  print(len(pipelines), 'need to be run')
  notebooks = [NotebookData("/Shared/InsertTableDeltaIntoDataLake", 60*60, { "ConnectionName" : p['connection_name'], "SnapshotFilename" : p['snapshot_filename'], "LoadStrategy" : p['load_strategy'], "DestTableName" : p['dest_table_name']}) for p in pipelines]
  res = parallelNotebooks(notebooks, concurrent)
  result = [i.result(timeout=3600) for i in res] # This is a blocking call.
  
  notebooks = [NotebookData("/Shared/CreateDeltaTable", 60*60, { "ConnectionName" : p['connection_name'], "DestTableName" : p['dest_table_name']}) for p in pipelines]
  res = parallelNotebooks(notebooks, concurrent)
  result = [i.result(timeout=3600) for i in res] # This is a blocking call.
  
  #notebooks = [NotebookData("/Shared/CreateTableViews", 60, { "ConnectionName" : p['connection_name'], "DestTableName" : p['dest_table_name']}) for p in pipelines]
  #res = parallelNotebooks(notebooks, concurrent)
  #result = [i.result(timeout=3600) for i in res] # This is a blocking call.
  
  print(result) 
  
  

# COMMAND ----------

get_all_metadata()
