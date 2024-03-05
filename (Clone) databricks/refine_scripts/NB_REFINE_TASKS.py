# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('../'))

import datetime
import pyspark.pandas as ps
from pyspark.sql.types import *
from modules.common import constants, functions

# Get the config for the specific table
js_config = functions.func_get_config(
  str_config_path = constants.CONST_CONFIG_PATH)['refined_tables']['tasks']

# Get the folder path of the latest ingested TaskPlanner ata
str_task_planner_src_path = functions.func_get_latest_path_from_date_hierarchy(
  str_path = js_config['src_path'][0],
  dbutil = dbutils)

# Get the folder path of the latest ingested Cxmra Tasks Data
str_cxmra_src_path = functions.func_get_latest_path_from_date_hierarchy(
  str_path = js_config['src_path'][1],
  dbutil = dbutils)

# Read task planner raw data and create temporary view: temp_task_planner.
spark.read.json(str_task_planner_src_path).createOrReplaceTempView('temp_task_planner')

# Read CXMRA tasks raw data and create temporary view: temp_cxmra.
df_raw_cxmra_excel = ps.read_excel(str_cxmra_src_path, 
  sheet_name = 'CDO-SSR', 
  header = 0
).to_spark().createOrReplaceTempView('temp_cxmra')



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_cxmra_preproc
# MAGIC AS
# MAGIC SELECT 
# MAGIC   cx.Summary as Title,
# MAGIC   'CXMRA BAU' AS TC_PlannerCode,
# MAGIC   cx.IssueKey as TaskID,
# MAGIC   'CbLSYAsOKUCI457rdCaIEmQAMH40' as BucketID,
# MAGIC   '' as Progress,
# MAGIC   cx.Status as ProgressDescription,
# MAGIC   b.Office365ID as CreatedByOffice365ID,
# MAGIC   cx.Created as CreatedDate,
# MAGIC   cx.Resolved as CompletedDate,
# MAGIC   cx.TimeSpent as TimeSpent
# MAGIC FROM temp_cxmra cx
# MAGIC INNER JOIN util_refined_db.members b
# MAGIC   ON cx.Assignee = b.AssignedTo
# MAGIC WHERE startswith(cx.Projectname, 'Generated') = FALSE

# COMMAND ----------

spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW temp_transformed_task_planner
AS
SELECT 
			current_timestamp() as `UploadDate`,
			'{constants.CONST_INGESTION_UPLOADER}' as `UploadBy`,
			Title,
			CASE WHEN CHARINDEX(']', Title) > 0 THEN
			REPLACE(LEFT(Title, CHARINDEX(']', Title)-1),'[','') ELSE
			REPLACE(Title, ']','') END AS TC_PlannerCode,
			TaskID,
			BucketID,
			CAST(Progress AS BIGINT) AS Progress,
			CASE WHEN Progress = '0' THEN 'Not Started' WHEN Progress = '50' THEN 'In Progress' WHEN Progress = '100' THEN 'COMPLETED'
				ELSE 'NOT RECOGNIZED PLEASE CONTACT ADMIN' END AS TC_ProgressDescription,
			CreatedByOffice365ID,
			CAST(CreatedDate AS DATE) AS CreatedDate,
			CAST(StartDate AS DATE) AS StartDate,
			CAST(DueDate AS DATE) AS DueDate,
			CAST(CompletedDate AS DATE) AS CompletedDate,
			CASE WHEN Progress = '100' AND DueDate IS NOT NULL THEN CAST(DueDate AS DATE)
				 WHEN Progress = '100' AND StartDate IS NOT NULL THEN CAST(StartDate AS DATE)
				 ELSE CAST(CompletedDate AS DATE) END AS TC_ProductiveDate,
            CASE WHEN CAST(`hours` as int) IS NOT NULL THEN(
                CASE when `hours` like '%,%'
                                    then 0
                                    else        
                                            (case when ((CHARINDEX('.',  `hours`) >=2 and len(`hours`) >=2)  or (CHARINDEX('.',  `hours`)= 0) and len(`hours`) >=1)
                                            then cast (`hours` as decimal(10,2))
                                            when CHARINDEX('.',  `hours`) =1 and len(`hours`) =1-- good
                                            then cast ( concat('0',`hours`,'0') as decimal(10,2))
                                            when CHARINDEX('.',  `hours`) =1 and len(`hours`) >=2 --good
                                            then cast ( concat('0',`hours`) as decimal(10,2))
                                            when  CHARINDEX('.',  `hours`)= len(`hours`)
                                            then cast ( concat(`hours`,'0') as decimal(10,2))
                                            else 0
                                            end)  
                                    end)
                        else 0
                        end as `Hours`

FROM temp_task_planner
UNION ALL
SELECT 
	current_timestamp() AS UploadDate,
	'{constants.CONST_INGESTION_UPLOADER}' AS UploadBy,
	Title,
	TC_PlannerCode,
	TaskID,
	BucketID,
	CASE WHEN CAST(Progress as int) IS NOT NULL
		THEN CAST(Progress AS BIGINT)*100 
		WHEN ProgressDescription = 'Done'
		THEN 100
		ELSE 0 END AS Progress,
	CASE WHEN (Progress = 0 OR ProgressDescription = 'To Do') THEN 'Not Started' 
			WHEN ProgressDescription in ('In Progress','In Review') THEN 'In Progress'
			WHEN Progress = 1 OR ProgressDescription = 'Done' THEN 'COMPLETED'
		ELSE 'NOT RECOGNIZED PLEASE CONTACT ADMIN' END AS TC_ProgressDescription,
	CreatedByOffice365ID,
	CAST(CreatedDate AS DATE) AS CreatedDate,
	CAST(NULL AS DATE) AS StartDate,
	CAST(NULL AS DATE) AS DueDate,
	CAST(CASE WHEN LTRIM(RTRIM(CompletedDate)) =''
		THEN NULL
		ELSE CompletedDate END AS DATE) AS CompletedDate,
	CASE WHEN ProgressDescription = 'Done' AND (CompletedDate IS NOT NULL AND LTRIM(RTRIM(CompletedDate)) != '') THEN CAST(CompletedDate AS DATE)
			WHEN ProgressDescription = 'Done' AND CreatedDate IS NOT NULL THEN CAST(CreatedDate AS DATE)
			ELSE NULL END AS TC_ProductiveDate,
	      CASE WHEN CAST(TimeSpent as int) IS NOT NULL 
		THEN CAST(CAST(TimeSpent AS FLOAT)/3600 AS DECIMAL(10,2))
		ELSE 0 END as `hours`
FROM temp_cxmra_preproc
""")


# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS util_refined_db.tasks(
      UploadDate TIMESTAMP,
      UploadBy STRING,
      Title STRING,
      TC_PlannerCode STRING,
      TaskID STRING,
      BucketID STRING,
      Progress BIGINT,
      TC_ProgressDescription STRING,
      CreatedByOffice365ID STRING,
      CreatedDate DATE,
      StartDate DATE,
      DueDate DATE,
      CompletedDate DATE,
      TC_ProductiveDate DATE,
      Hours DECIMAL(10,2)
    )
    LOCATION '{js_config['output_path']}'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO util_refined_db.tasks tgt USING temp_transformed_task_planner src
# MAGIC ON tgt.TaskID = src.TaskID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# 676

# COMMAND ----------


