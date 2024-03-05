# Databricks notebook source
list_paths = dbutils.fs.ls('abfs://raw@gdmsutiladls.dfs.core.windows.net/QeDemo/')

# COMMAND ----------

import pyspark.sql.functions as F

df_combined = None

for file_info in list_paths:
    # Read CSV from the path
    df_read_csv = spark.read.csv(file_info.path, header=True)

    # Create a file name column
    df_read_csv = df_read_csv.withColumn('FileName', F.lit(file_info.name))
    
    # Combine the extracted CSV to the df_combined variable
    if df_combined is None:
        df_combined = df_read_csv
    else:
        df_combined = df_combined.unionAll(df_read_csv)

display(df_combined)

# COMMAND ----------

df_combined.createOrReplaceTempView('temp_members')


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_validation_results
# MAGIC AS
# MAGIC SELECT DISTINCT CONCAT(GivenName, ' ', Surname) as FullName FROM temp_members

# COMMAND ----------

from datetime import datetime

df_result = spark.sql("SELECT * FROM temp_validation_results")

# result_{date today}
str_file_path = 'abfs://raw@gdmsutiladls.dfs.core.windows.net/QeResult/result_' + datetime.today().strftime('%Y-%m-%d')

df_result.repartition(1).write.csv(str_file_path, header=True)

# COMMAND ----------


