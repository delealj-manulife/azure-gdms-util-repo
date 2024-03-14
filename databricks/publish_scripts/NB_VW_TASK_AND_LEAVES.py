# Databricks notebook source
spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW temp_Main
AS
SELECT 
    S.TaskID,
    S.TC_ProductiveDate AS `Date`,
    B.BucketName as Category,
    S.TC_PlannerCode AS PlannerCode,
    S.`Hours` as `Hours`,
    M.AssignedTo,
    M.RoleGroup as `Role`,
    M.Squad,
    M.ManagerName,
    M.WorkdayID
    FROM (SELECT DISTINCT * FROM util_refined_db.tasks) S
    INNER JOIN util_refined_db.members M
        ON M.Office365ID = S.CreatedByOffice365ID
    LEFT OUTER JOIN util_refined_db.dim_date D --need dim_date
        ON D.DateKey = CAST(REPLACE(S.TC_ProductiveDate,'-','') AS BIGINT)
    LEFT OUTER JOIN util_refined_db.buckets B
        ON B.BucketID = S.BucketID
    WHERE S.Progress = 100 AND S.TC_ProductiveDate IS NOT NULL
    UNION ALL
    select 
	  '' AS TaskID,
    S.TimeOffDate AS `Date`,
    S.TimeOffTypeForTimeOffEntry as Category,
    S.TimeOffTypeForTimeOffEntry AS PlannerCode,
    (S.`TC_Hours`*`Unit`) as `Hours`,
    M.AssignedTo,
    M.RoleGroup as `Role`,
    M.Squad,
    M.ManagerName,
    M.WorkdayID
    from 
    util_refined_db.leaves S
    INNER JOIN util_refined_db.members M
        ON M.WorkdayID = S.EmployeeID
    JOIN util_refined_db.dim_date D --need dim_date
        ON D.DateKey = CAST(REPLACE(S.TimeOffDate,'-','') AS BIGINT)
    WHERE S.TimeOffDate IS NOT NULL
	      AND TimeOffTypeForTimeOffEntry NOT LIKE 'Business Unit Holiday%' 
	--AND TimeOffTypeForTimeOffEntry NOT LIKE 'Vacation%' AND TimeOffTypeForTimeOffEntry <> 'Extra Personal Days'
 """)

# COMMAND ----------

spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW temp_PlannedLeaves
AS
SELECT 
    CONCAT(S.EmployeeID, SUBSTRING(CAST(D.DateKey AS STRING),1,6)) as WorkdayIDYearMonthKey,
    SUM(S.`TC_Hours`*Unit) as `TotalHoursToDeduct`
    from 
    util_refined_db.leaves S
    JOIN util_refined_db.dim_date D --need dim_date
        ON D.DateKey = CAST(REPLACE(S.TimeOffDate,'-','') AS BIGINT)
    WHERE S.TimeOffDate IS NOT NULL
    AND RTRIM(LTRIM(UPPER(NVL(S.TimeOffTypeForTimeOffEntry, '')))) NOT IN ('SICK DAY', 'EMERGENCY LEAVE', 'PHILIPPINES UNPAID TIME OFF')
	     --AND UNIT <> -1
    GROUP BY CONCAT(S.EmployeeID, SUBSTRING(CAST(D.DateKey AS STRING),1,6))
""")

# COMMAND ----------

spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW temp_ExpectedHours
AS
    SELECT
        CONCAT(S.WorkdayID, SUBSTRING(CAST(D.DateKey AS STRING),1,6)) as WorkdayIDYearMonthKey,
        COUNT(1) as TotalWorkingDays,
        COUNT(1) * 7.5 as TargetHours
    FROM 
		util_refined_db.members S
		JOIN util_refined_db.dim_date  D --dim_date
			ON DateYear >= 2021 AND IsWeekend = 0
		AND DateKey <= CONCAT( YEAR(GETDATE()), RIGHT(CONCAT( '0', MONTH(GETDATE())),2), RIGHT(CONCAT( '0', DAY(GETDATE())),2))
		AND DateKey BETWEEN CAST(REPLACE(S.StartDate,'-','') AS BIGINT) AND CAST(REPLACE(S.EndDate,'-','') AS BIGINT)
    GROUP BY
		CONCAT(S.WorkdayID, SUBSTRING(CAST(D.DateKey AS STRING),1,6))
""")

# COMMAND ----------

spark.sql(f"""       
CREATE OR REPLACE TEMPORARY VIEW Final_tbl
AS
    SELECT DISTINCT
	S.TaskID,
    S.`Date` AS `Date`,
    D.DateKey,
    D.DateYear,
    D.DateMonthName,
    D.DateMonthNum,
    SUBSTRING(CAST(D.DateKey AS STRING),1,6) as DateYearMonthKey,
    S.Category,
    S.PlannerCode,
    CASE WHEN S.Category = 'Admin' AND S.PlannerCode = 'LEADS' 
		THEN 'Productive'
		WHEN S.Category = 'Admin' AND S.PlannerCode != 'LEADS'
		THEN 'Nonproductive'
		ELSE P.Type
		END AS ProductivityType,
    S.AssignedTo,
    S.`Role` as `Role`,
    S.Squad,
    S.ManagerName,
    S.WorkdayID,
    S.`Hours` as `Hours`,
    CONCAT(S.WorkdayID, SUBSTRING(CAST(D.DateKey AS STRING),1,6)) as WorkdayIDYearMonthKey,
    E.TargetHours,
    NVL(PL.TotalHoursToDeduct,0) TotalHoursToDeduct, 
	E.TargetHours -  NVL(PL.TotalHoursToDeduct,0) as FinalTargetHours
        FROM temp_Main S
    LEFT OUTER JOIN util_refined_db.dim_date D --dim_date
    ON D.DateKey = CAST(REPLACE(S.`Date`,'-','') AS BIGINT)
    LEFT OUTER JOIN util_refined_db.productivity_types P
    ON P.Category = S.Category
    LEFT OUTER JOIN temp_ExpectedHours E
    ON E.WorkdayIDYearMonthKey = CONCAT(S.WorkdayID, SUBSTRING(CAST(D.DateKey AS STRING),1,6))
    LEFT OUTER JOIN temp_PlannedLeaves PL
    ON PL.WorkdayIDYearMonthKey = CONCAT(S.WorkdayID, SUBSTRING(CAST(D.DateKey AS STRING),1,6))
    WHERE D.DateYear >= 2021 --= YEAR(GETDATE())
""")

# COMMAND ----------

df_vw_task_and_leaves = spark.sql(f"""
  SELECT 
    `Date`,
    DateKey,
    DateYear,
    DateMonthName,
    DateMonthNum,
    DateYearMonthKey,
    Category,
    PlannerCode,
    ProductivityType,
    AssignedTo,
    `Role`,
    Squad,
    ManagerName,
    WorkdayID,
    `Hours`,
    WorkdayIDYearMonthKey,
    TargetHours,
    TotalHoursToDeduct, 
	FinalTargetHours
    FROM Final_tbl
""")

# Output transformed dataframe
display(df_vw_task_and_leaves)
df_vw_task_and_leaves.printSchema()

# Save the transformed dataframe to delta table
df_vw_task_and_leaves.write.option("path", f"abfss://published@gdmsutiladls.dfs.core.windows.net/util_published_db/vw_task_and_leaves/")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_published_db.vw_task_and_leaves")

# COMMAND ----------


