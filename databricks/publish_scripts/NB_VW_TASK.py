# Databricks notebook source
df_vw_task = spark.sql(f"""
  SELECT DISTINCT
	S.Title,
	S.TC_ProductiveDate AS `Date`,
	D.DateKey,
	D.DateYear,
	D.DateMonthName,
	D.DateMonthNum,
	SUBSTRING(CAST(D.DateKey AS STRING),1,6) as DateYearMonthKey,
	B.BucketName as Category,
	CASE WHEN B.BucketName = 'Admin' AND S.TC_PlannerCode = 'LEADS' 
		THEN 'Productive'
		WHEN B.BucketName = 'Admin' AND S.TC_PlannerCode != 'LEADS'
		THEN 'Nonproductive'
		ELSE P.Type
		END AS ProductivityType,
	S.`Hours` as `Hours`,
	M.AssignedTo,
	M.RoleGroup as `Role`,
	M.Squad,
	M.ManagerName,
	M.WorkdayID,
	PC.Code AS PlannerCodeKey,
	S.TC_PlannerCode AS PlannerCode,
	PC.Name AS PlannerName,
	CASE WHEN PC.Code IS NULL THEN 'No'
		ELSE 'Yes' END as IsPlannerCodeCorrect
	from 
	util_refined_db.tasks S
	JOIN util_refined_db.members M
		ON M.Office365ID = S.CreatedByOffice365ID
	JOIN util_refined_db.dim_Date D --dim_date
		ON D.DateKey = CAST(REPLACE(S.TC_ProductiveDate,'-','') AS BIGINT)
	JOIN util_refined_db.buckets B
		ON B.BucketID = S.BucketID
	LEFT JOIN util_refined_db.planner_codes PC
		ON PC.Code = CONCAT('[',S.TC_PlannerCode,']')
	JOIN util_refined_db.productivity_types P
	ON P.Category = B.BucketName
	WHERE S.Progress = 100 AND S.TC_ProductiveDate IS NOT NULL
	AND D.DateYear >= 2021
""")

# Output transformed dataframe
display(df_vw_task)
df_vw_task.printSchema()

# Save the transformed dataframe to delta table
df_vw_task.write.option("path", f"abfss://published@gdmsutiladls.dfs.core.windows.net/util_published_db/vw_task/")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_published_db.vw_task")

# COMMAND ----------



# COMMAND ----------


