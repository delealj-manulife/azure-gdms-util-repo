# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('../'))

import pyspark.pandas as ps
import numpy as np
import datetime
from pyspark.sql.types import *

# Import custom modules
from modules.common import constants, functions

# Get the config for the specific table
js_config = functions.func_get_config(
  str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['buckets']
    
df_raw_gdms_reference_excel = ps.read_excel(js_config['src_path'], 
  sheet_name = 'Buckets', 
  header = 0
)

# Rename columns
df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.rename(
  columns={
    "Bucket Name": "BucketName", 
    "Bucket ID": "BucketID"
  })

# Drop columns
df_raw_gdms_reference_excel = df_raw_gdms_reference_excel.drop(columns=['Source Planner'])

# Add active flag and set to 1
df_raw_gdms_reference_excel['IsActive'] = 1

# Add audit columns
df_raw_gdms_reference_excel['UploadDate'] = datetime.datetime.now()
df_raw_gdms_reference_excel['UploadBy'] = 'SVC_MBPSDW'

# Save the transformed dataframe to delta table
display(df_raw_gdms_reference_excel)
df_raw_gdms_reference_excel.to_delta(js_config['output_path'],  mode='overwrite')

functions.func_create_external_delta_table(
  spark_context = spark,
  str_db_name = js_config['dbx_db_name'],
  str_tbl_name = js_config['dbx_tbl_name'],
  str_delta_path = js_config['output_path']
)

# COMMAND ----------


