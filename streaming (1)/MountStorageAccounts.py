# Databricks notebook source
def mount_storage_account(storage_account_name, storage_account_container_name, access_key):
    mount_path = f"/mnt/{storage_account_name}/{storage_account_container_name}"
    mount_source = f"wasbs://{storage_account_container_name}@{storage_account_name}.blob.core.windows.net"
    mount_config_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
    
    print(mount_path, mount_source, mount_config_key)
    
    try:
        dbutils.fs.mount(
            source = mount_source,
            mount_point = mount_path,
            extra_configs = {mount_config_key : access_key}
          )
        
        return True
    except Exception as ex: 
        if not 'Directory already mounted' in str(ex): 
            raise ex
        print ("Directory already mounted...")
        return True

mount_storage_account("gdpcorerefimplbronzesag2", "logs", "iwX90qf8iYoui6h+VhvR91/OuHMnHPEsz4O4nu2Q/kEec3a22IZL1cPeoARYsKG6s4FaKN9buFet+AStCzuBzw==")
mount_storage_account("gdpcorerefimplbronzesag2", "bronze", "iwX90qf8iYoui6h+VhvR91/OuHMnHPEsz4O4nu2Q/kEec3a22IZL1cPeoARYsKG6s4FaKN9buFet+AStCzuBzw==")
mount_storage_account("gdpcorerefimplrawsagen2", "ingest", "zDQosM9avhaVSs8hPBZFmFjtf34v8P6b4JUEq3vrnjB56fPl3ldZIlPrleyK8rdpHJzWABvQEG+dciYKbUdY0Q==")
mount_storage_account("gdpcorerefimplrawsagen2", "delta", "zDQosM9avhaVSs8hPBZFmFjtf34v8P6b4JUEq3vrnjB56fPl3ldZIlPrleyK8rdpHJzWABvQEG+dciYKbUdY0Q==")

# COMMAND ----------

# MAGIC %md
# MAGIC old code below

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
print(get_dir_content('/mnt/gdpcorerefimplrawsagen2/ingest'))
print(get_dir_content('/mnt/gdpcorerefimplbronzesag2/bronze'))

# COMMAND ----------





from datetime import datetime, timedelta
import json
import uuid
def bail(ex=None, step_name=None):
  params['error'] = str(ex)
  params['last_error_step_name'] = step_name
  # TODO log parameters to remote
  if ex:
    print(ex, params)
    raise ex
  else:
    dbutils.notebook.exit(json.dumps(params))


    

params = {}
params['run_id'] = str(uuid.uuid4())
params['run_start'] = datetime.utcnow().isoformat() + 'Z'
params['storage_account_name'] = dbutils.widgets.get("storage_account_name")
params['storage_account_container_name'] = dbutils.widgets.get("storage_account_container_name")
params['storage_account_path'] = dbutils.widgets.get("storage_account_path")
params['mount_path'] = f"/mnt/{params['storage_account_name']}{params['storage_account_path']}"
params['mount_source'] = f"wasbs://{params['storage_account_container_name']}@{params['storage_account_name']}.blob.core.windows.net"
params['mount_config_key'] = f"fs.azure.account.key.{params['storage_account_name']}.blob.core.windows.net"
print(params)

# COMMAND ----------

try:
  dbutils.fs.mount(
    source = params['mount_source'],
    mount_point = params['mount_path'],
    extra_configs = {params['mount_config_key'] : "8LvOcHOBuoET1GhTKUmU/SjE/Uhd2WG7goS/XGgerC6Gzil+71Lf+YJOzNXaxEndW3Fq7WqL1H/bml3mp7mA9g=="}
  )
  params['location_mounted'] = True
except Exception as ex: 
  if not 'Directory already mounted' in str(ex): 
    bail(ex, 'location_mount') # don't bail if the location already exists
  params['location_already_mounted'] = True
  params['location_mounted'] = True

print(params)
