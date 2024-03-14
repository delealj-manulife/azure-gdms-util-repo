# Databricks notebook source
spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW temp_Main
    AS
	 SELECT DateYearMonthKey
		,DateYear
		,DateMonthName
		,AssignedTo
		,WorkdayID
		,Squad
		,ManagerName
		,SUM(`Hours`) AS TotalHours
		,FinalTargetHours
		FROM util_published_db.vw_task_and_leaves
		WHERE DateYear = YEAR(current_timestamp()) --AND [DateMonthNum] = MONTH(SYSDATETIME())
		GROUP BY DateYearMonthKey
		,DateYear
		,DateMonthName
		,AssignedTo
		,WorkdayID
		,Squad
		,ManagerName
		,FinalTargetHours
""")

df_vw_target_hours = spark.sql(f"""  
    SELECT DateYearMonthKey
		,DateYear
		,DateMonthName
		,AssignedTo
		,WorkdayID
		,Squad
		,ManagerName
		,TotalHours
		,FinalTargetHours
FROM temp_Main
    WHERE TotalHours < FinalTargetHours *.75
""")

# Output transformed dataframe
display(df_vw_target_hours)
df_vw_target_hours.printSchema()

# Save the transformed dataframe to delta table
df_vw_target_hours.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_published_db/vw_target_hours/")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_published_db.vw_target_hours")

# COMMAND ----------



# COMMAND ----------


