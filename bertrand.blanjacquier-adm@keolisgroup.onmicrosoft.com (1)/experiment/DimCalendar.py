# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

def chkmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return True
    else:
        return False

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

if not chkmount("/mnt/standard"):
    dbutils.fs.mount(source = "abfss://standard@dkbmtbmeuwdls01.dfs.core.windows.net",mount_point = "/mnt/standard",extra_configs = configs)

beginDate = '2000-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)



# COMMAND ----------

beginDate = '2000-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dim_calendar;
# MAGIC create or replace table BI.dim_calendar
# MAGIC using delta
# MAGIC location '/mnt/standard/BI/dim_calendar'
# MAGIC as select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateInt,
# MAGIC   CalendarDate,
# MAGIC   year(calendarDate) AS CalendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as CalendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(calendarDate) as DayOfWeek,
# MAGIC   weekday(calendarDate) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as DayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as DayOfYear,
# MAGIC   weekofyear(calendarDate) as WeekOfYearIso,
# MAGIC   quarter(calendarDate) as QuarterOfYear,
# MAGIC   /* Use fiscal periods needed by organization fiscal calendar */
# MAGIC   case
# MAGIC     when month(calendarDate) >= 10 then year(calendarDate) + 1
# MAGIC     else year(calendarDate)
# MAGIC   end as FiscalYearOctToSep,
# MAGIC   (month(calendarDate) + 2) % 12 + 1 as FiscalMonthOctToSep,
# MAGIC   case
# MAGIC     when month(calendarDate) >= 7 then year(calendarDate) + 1
# MAGIC     else year(calendarDate)
# MAGIC   end as FiscalYearJulToJun,
# MAGIC   (month(calendarDate) + 5) % 12 + 1 as FiscalMonthJulToJun
# MAGIC from
# MAGIC   dates
