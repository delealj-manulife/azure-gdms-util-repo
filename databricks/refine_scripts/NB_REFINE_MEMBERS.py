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
  str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['members']

df_raw_gdms_reference_excel_members = ps.read_excel(js_config['src_path'], 
  sheet_name = 'Members v2', 
  header = 0
).to_spark().createOrReplaceTempView("temp_members")

# Get the config for the specific table
js_config = functions.func_get_config(
  str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['roles']

df_raw_gdms_reference_excel_roles = ps.read_excel(js_config['src_path'], 
  sheet_name = 'Roles', 
  header = 0
).to_spark().createOrReplaceTempView("temp_roles")

#curation raw to refined
df_raw_gdms_reference_excel_members_final = spark.sql(f"""
  SELECT 
  current_timestamp() `UploadDate`,
  '{constants.CONST_INGESTION_UPLOADER}' `UploadBy`
  ,m.Office365ID
  ,CAST(m.WorkdayID AS BIGINT) WorkdayID
  ,m.GivenName
  ,m.Surname
  ,`AssignedTo (Display Name in Teams)` as AssignedTo
  ,m.JobTitle
  ,r.Group RoleGroup
  ,m.ManagerName
  ,m.Squad
  ,CAST(m.StartDate AS DATE) StartDate
  ,CAST(m.EndDate AS DATE) EndDate
  ,CASE WHEN m.EndDate = '9999-12-31' THEN CAST('1' AS BIGINT) ELSE CAST('0' AS BIGINT) END AS IsActive
    FROM temp_members m
      JOIN temp_roles r 
        on m.JobTitle = r.JobTitle
""")

# Output transformed dataframe
display(df_raw_gdms_reference_excel_members_final)
df_raw_gdms_reference_excel_members_final.printSchema()

# Save the transformed dataframe to delta table
df_raw_gdms_reference_excel_members_final.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/members/")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_refined_db.members")

# COMMAND ----------


