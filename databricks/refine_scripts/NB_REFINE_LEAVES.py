# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('../'))

import pyspark.pandas as ps
import numpy as np
import datetime
from pyspark.sql.types import *
from modules.common import constants, functions

# Get the config for the specific table
js_config = functions.func_get_config(
  str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['leaves']

# Get the folder path of the latest ingested Leaves Data
str_leaves_src_path = functions.func_get_latest_path_from_date_hierarchy(
  str_path = js_config['src_path'],
  dbutil = dbutils)

# Read leaves raw data and create temporary view: temp_leaves
df_raw_leaves_excel = ps.read_excel(str_leaves_src_path, 
  sheet_name = 'All Worker Time Off', 
  header = 0
).to_spark().createOrReplaceTempView("temp_leaves")

#curation raw to refined
df_raw_leaves_excel = spark.sql(f"""
  SELECT 
  current_timestamp() `UploadDate`,
  '{constants.CONST_INGESTION_UPLOADER}' `UploadBy`
  ,CAST(TimeOffDate AS DATE) TimeOffDate
  ,date_format(TimeOffDate, 'EEEE') DayoftheWeek
  ,cast(split(Worker, "[(]")[0] as string) Worker
  ,cast(split(split(Worker, "[(]")[1], "[)]")[0] as bigint) EmployeeID
  ,cast(TimeOffAbsenceTable as string) TimeOffAbsenceTable
  ,cast(`Type` as string) TimeOffTypeforTimeOffEntry
  ,cast(Approved AS DECIMAL(10,2)) Unit
  ,cast('7.5' AS DECIMAL(10,2)) TC_Hours
  ,UnitofTime
  FROM temp_leaves
""")

# Output transformed dataframe
display(df_raw_leaves_excel)
df_raw_leaves_excel.printSchema()

# Save the transformed dataframe to delta table
df_raw_leaves_excel.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/leaves/")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_refined_db.leaves")

# COMMAND ----------



# COMMAND ----------


