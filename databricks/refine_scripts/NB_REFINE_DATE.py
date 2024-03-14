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
	str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['date']

df_raw_dim_date = ps.read_csv(js_config['src_path'], 
  sheet_name = 'date', 
  header = 0
).to_spark().createOrReplaceTempView("temp_dim_date")

df_raw_dim_date = spark.sql(f"""
  SELECT * from temp_dim_date 
  """)

#df_raw_date = spark.read.format("csv").option("header", "true").load("abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/date.csv/").to_spark().#createOrReplaceTempView("temp_dim_date")

# Read leaves raw data and create temporary view: temp_leaves
'''df_raw_leaves_excel = ps.read_excel(str_leaves_src_path, 
  sheet_name = 'date', 
  header = 0
).to_spark().createOrReplaceTempView("temp_leaves")'''

# Output transformed dataframe
display(df_raw_dim_date)
df_raw_dim_date.printSchema()

# Save the transformed dataframe to delta table
df_raw_dim_date.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/dim_date/")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_refined_db.dim_date")

# COMMAND ----------

# Output transformed dataframe
display(df_raw_dim_date)
#df_raw_dim_date.printSchema()

# Save the transformed dataframe to delta table
df_raw_dim_date.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/date/")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_refined_db.date")

# COMMAND ----------


