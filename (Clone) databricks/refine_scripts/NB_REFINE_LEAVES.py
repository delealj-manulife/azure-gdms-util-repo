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
).to_spark().createOrReplaceTempView('temp_leaves')

display(df_raw_leaves_excel)

#Rename the AssignedTo column
#df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(columns={"TimeOffEntry": "TimeOffDate"})
#df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(columns={"EnteredOn": "TimeOffDate"})
#Worker split worker
#EmployeeID split worker
#TimeOffAbsenceTable as is
#df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(columns={"Type": "TimeOffTypeforTimeOffEntry"})
#df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(columns={"Approved": "Unit"})
#TC_Hours / 7.5
#UnitofTime

#Rename the AssignedTo column
#df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(columns={"AssignedTo (Display Name in Teams)": "AssignedTo"})

# Add audit columns
#df_raw_gdms_reference_excel['UploadDate'] = datetime.datetime.now()
#df_raw_gdms_reference_excel['UploadBy'] = 'SVC_MBPSDW'

# Add active flag per member
#df_raw_gdms_reference_excel['IsActive'] = df_raw_gdms_reference_excel['EndDate'].map(lambda x: 1 if x == '9999-12-31' else 0)

# Convert StartDate and EndDate to date data type
#df_raw_gdms_reference_excel['StartDate'] = df_raw_gdms_reference_excel['StartDate'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()) 
#df_raw_gdms_reference_excel['EndDate'] = df_raw_gdms_reference_excel['EndDate'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()) 

####

# Save the transformed dataframe to delta table
#display(df_raw_leaves_excel)
#df_raw_leaves_excel.to_delta(js_config['output_path'],  mode='overwrite')

#functions.func_create_external_delta_table(
  #spark_context = spark,
  #str_db_name = js_config['dbx_db_name'],
  #str_tbl_name = js_config['dbx_tbl_name'],
  #str_delta_path = js_config['output_path']
#)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_leaves
# MAGIC AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as `UploadDate`
# MAGIC   ,'{constants.CONST_INGESTION_UPLOADER}' as `UploadBy`
# MAGIC   ,CAST(lv.EnteredOn AS DATE) as TimeOffDate
# MAGIC   --,[DayoftheWeek]
# MAGIC   ,lv.Worker
# MAGIC   --,lv.EmployeeID
# MAGIC   ,lv.TimeOffAbsenceTable
# MAGIC   ,lv.Type as TimeOffTypeforTimeOffEntry
# MAGIC   --,CAST(lv.Unit AS DECIMAL(10,2)) as Unit
# MAGIC   --,CAST(lv.TC_Hours = '7.5' AS DECIMAL(10,2)) as TC_Hours
# MAGIC   ,lv.UnitofTime as UnitofTime
# MAGIC FROM temp_leaves lv

# COMMAND ----------

display(df_raw_leaves_excel)
