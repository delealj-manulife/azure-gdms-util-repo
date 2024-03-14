# Databricks notebook source
'''import sys, os
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
'''

#curation raw to refined
df_dim_date = spark.sql(f"""
--CREATE PROCEDURE [ODS].[USP_LOAD_TO_DIM_DATE]
--AS
BEGIN
	SET NOCOUNT ON
		BEGIN TRY

	--  DECLARE VARIABLES:  --
	DECLARE @CTR_DATE AS DATE, 
		@END_DATE AS DATE, 
		@LATEST_YEAR AS INT,
		@ROLLBACK AS BIT = 0,
		@STATUS AS VARCHAR(2500),
		@EffDate AS DATETIME2(7) = CAST('1900-01-01' AS DATETIME2(7)),
		@ExpDate AS DATETIME2(7) = CAST('9999-12-31' AS DATETIME2(7))
	
	--  SET INIT VALUES OF VARIABLES:  --
	SET @STATUS = 'Set Initial Value for Start Date for insert looping.'
	SET @LATEST_YEAR = ISNULL((	SELECT MAX(DateYear) FROM util_refined_db.dim_date 
	WHERE DateYear < (SELECT MAX(DateYear) FROM util_refined_db.dim_date)), 1955)
	SET @CTR_DATE = CAST(('JANUARY 01, ' + CAST(@LATEST_YEAR AS VARCHAR(25))) AS DATE)
	
	SET @STATUS = 'Set Initial Value for End Date for insert looping = (YEAR TODAY + 10).'
	SET @END_DATE = CAST('JANUARY 01, ' + CAST((YEAR(SYSDATETIME()) + 2) AS VARCHAR(10)) AS DATE)
	
	--  INITIALIZE VALUE:  MIN DATE:  EFFECTIVE DATE  --
	SET @STATUS = 'Set Initial Value for Minimum Date - for Effective Date.'
	
	--  DEBUGGER:  --
	SELECT @Status AS Status, @CTR_DATE AS CTR_DATE, @END_DATE AS END_DATE
	
	--  INSERT INITIAL DATE HERE:  INSERT EFFECTIVE AND EXPIRY DATES  --
	
	BEGIN TRAN
	SET @ROLLBACK = 1
			
		SET @STATUS = 'INSERT INITIAL VALUE: ' + CAST(@EffDate AS VARCHAR(50)) + ' and '  + CAST(@ExpDate AS VARCHAR(50))
			
		INSERT INTO util_refined_db.dim_date 
		(DateKey,
			IsCurrent,
			EffectiveDate,
			ExpiryDate,
			CreatedDate,
			CreatedBy,
			ModifiedDate,
			ModifiedBy,
			DateShort,
			DateYear,
			DateQuarter,
			DateQuarterName,
			DateMonthNum,
			DateMonthName,
			DateDayNum,
			DateDayName,
			IsWeekend,
			IsHoliday_HK,
			IsHoliday_ID,
			IsHoliday_JP,
			IsHoliday_PH,
			IsHoliday_SG,
			IsHoliday_VN,
			IsHoliday_CDN,
			IsHoliday_USA)
		SELECT * 
		FROM (
			SELECT FORMAT(CAST(@EffDate AS DATE), 'yyyyMMdd') AS DateKey, 
				1 AS IsCurrent,
				CAST(@EffDate AS DATE) AS EffectiveDate, 
				CAST(@ExpDate AS DATE) AS ExpiryDate, 
				SYSDATETIME() AS CreatedDate, 
				SYSTEM_USER AS CreatedBy, 
				SYSDATETIME() AS ModifiedDate,  
				SYSTEM_USER AS ModifiedBy,
				CAST(@EffDate AS DATE) AS DateShort, 
				YEAR(@EffDate) AS DateYear, 
				DATEPART(QUARTER, @EffDate) AS DateQuarter, 
				'Q' + CAST(DATEPART(QUARTER, @EffDate) AS VARCHAR(2)) + '-' +
					CAST(YEAR(@EffDate) AS VARCHAR(4)) AS DateQuarterName, 
				MONTH(CAST(@EffDate AS DATE)) AS DateMonthNum, 
				DATENAME(MONTH, @EffDate) AS DateMonthName, 
				DAY(@EffDate) AS DateDayNum, 
				DATENAME(DW, @EffDate) AS DateDayName, 
				CASE DATEPART(DW, @EffDate) 
					WHEN 1 THEN 1 
					WHEN 7 THEN 1 
					ELSE 0 END AS IsWeekend, 
				0 AS IsHoliday_HK, 
				0 AS IsHoliday_ID, 
				0 AS IsHoliday_JP, 
				0 AS IsHoliday_PH, 
				0 AS IsHoliday_SG, 
				0 AS IsHoliday_VN, 
				0 AS IsHoliday_CDN, 
				0 AS IsHoliday_USA
			UNION ALL
			SELECT FORMAT(CAST(@ExpDate AS DATE), 'yyyyMMdd') AS DateKey, 
				1 AS IsCurrent,
				CAST(@EffDate AS DATE) AS EffectiveDate, 
				CAST(@ExpDate AS DATE) AS ExpiryDate, 
				SYSDATETIME() AS CreatedDate, 
				SYSTEM_USER AS CreatedBy, 
				SYSDATETIME() AS ModifiedDate,  
				SYSTEM_USER AS ModifiedBy,
				CAST(@ExpDate AS DATE) AS DateShort, 
				YEAR(@ExpDate) AS DateYear, 
				DATEPART(QUARTER, @ExpDate) AS DateQuarter, 
				'Q' + CAST(DATEPART(QUARTER, @ExpDate) AS VARCHAR(2)) + '-' +
					CAST(YEAR(@ExpDate) AS VARCHAR(4)) AS DateQuarterName, 
				MONTH(CAST(@ExpDate AS DATE)) AS DateMonthNum, 
				DATENAME(MONTH, @ExpDate) AS DateMonthName, 
				DAY(@ExpDate) AS DateDayNum, 
				DATENAME(DW, @ExpDate) AS DateDayName, 
				CASE DATEPART(DW, @ExpDate) 
					WHEN 1 THEN 1 
					WHEN 7 THEN 1 
					ELSE 0 END AS IsWeekend, 
				0 AS IsHoliday_HK, 
				0 AS IsHoliday_ID, 
				0 AS IsHoliday_JP, 
				0 AS IsHoliday_PH, 
				0 AS IsHoliday_SG, 
				0 AS IsHoliday_VN, 
				0 AS IsHoliday_CDN, 
				0 AS IsHoliday_USA
		) AS TBL
		WHERE NOT EXISTS (SELECT DIM.DateShort 
			FROM util_refined_db.dim_date AS DIM
			WHERE TBL.DateShort = DIM.DateShort)
		
	COMMIT TRAN 
	SET @ROLLBACK = 0
		
	--  PREP. RAW TABLE  --

	SET @STATUS = 'Prepare RAW Table.'
	
	BEGIN TRAN
	SET @ROLLBACK = 1
				
		SELECT * INTO #TMP_DIM_DATE 
		FROM util_refined_db.dim_date 			
		WHERE 1 = 2		

	COMMIT TRAN
	SET @ROLLBACK = 0
	
	--  PROCESS: INSERT DATES FOR THE YEAR:  --

	WHILE @CTR_DATE < @END_DATE
	BEGIN
		SET @STATUS = 'While Loop: ' + CAST(@CTR_DATE AS VARCHAR(50))		
			
		--  CHECK IF DATE EXISTS:  --
		IF (SELECT COUNT(*) 
			FROM util_refined_db.dim_date
			WHERE DateKey = FORMAT(@CTR_DATE, 'yyyyMMdd')) > 0
		BEGIN
			--  UPDATE:  --
			SET @STATUS = 'UPDATE: ' + CAST(@CTR_DATE AS VARCHAR(50))
			
			BEGIN TRAN
			SET @ROLLBACK = 1
				
				UPDATE util_refined_db.dim_date
				SET DateShort = @CTR_DATE, 
					DateYear = YEAR(@CTR_DATE), 
					DateQuarter = DATEPART(QUARTER, @CTR_DATE), 
					DateQuarterName = 'Q' + CAST(DATEPART(QUARTER, @CTR_DATE) AS VARCHAR(2)) + '-' + 
						CAST(YEAR(@CTR_DATE) AS VARCHAR(4)), 
					DateMonthNum = MONTH(@CTR_DATE), 
					DateMonthName = DATENAME(MONTH, @CTR_DATE), 
					DateDayNum = DAY(@CTR_DATE), 
					DateDayName = DATENAME(DW, @CTR_DATE), 
					IsWeekend = CASE DATEPART(DW, @CTR_DATE) 
						WHEN 1 THEN 1 
						WHEN 7 THEN 1 
						ELSE 0 END,
					ModifiedDate = SYSDATETIME(),
					ModifiedBy = SYSTEM_USER
				WHERE DateKey = FORMAT(@CTR_DATE, 'yyyyMMdd')
				
			COMMIT TRAN
			SET @ROLLBACK = 0
			
		END
		ELSE
		BEGIN 
			--  INSERT:  --
			BEGIN TRAN
				SET @ROLLBACK = 1
				
				SET @STATUS = 'INSERT: ' + CAST(@CTR_DATE AS VARCHAR(30))

				INSERT INTO #TMP_DIM_DATE 
				(DateKey,
					IsCurrent,
					EffectiveDate,
					ExpiryDate,
					CreatedDate,
					CreatedBy,
					ModifiedDate,
					ModifiedBy,
					DateShort,
					DateYear,
					DateQuarter,
					DateQuarterName,
					DateMonthNum,
					DateMonthName,
					DateDayNum,
					DateDayName,
					IsWeekend,
					IsHoliday_HK,
					IsHoliday_ID,
					IsHoliday_JP,
					IsHoliday_PH,
					IsHoliday_SG,
					IsHoliday_VN,
					IsHoliday_CDN,
					IsHoliday_USA)
				SELECT FORMAT(CAST(@CTR_DATE AS DATE), 'yyyyMMdd') AS DateKey, 
					1 AS IsCurrent,
					@EffDate AS EffectiveDate,
					@ExpDate AS ExpiryDate,
					SYSDATETIME() AS CreatedDate, 
					SYSTEM_USER AS CreatedBy, 
					SYSDATETIME() AS ModifiedDate,  
					SYSTEM_USER AS ModifiedBy,
					CAST(@CTR_DATE AS DATE) AS DateShort, 
					YEAR(CAST(@CTR_DATE AS DATE)) AS DateYear, 
					DATEPART(QUARTER, @CTR_DATE) AS DateQuarter, 
					'Q' + CAST(DATEPART(QUARTER, CAST(@CTR_DATE AS DATE)) AS VARCHAR(2)) + '-' + 
						CAST(YEAR(@CTR_DATE) AS VARCHAR(4)) AS DateQuarterName, 
					MONTH(CAST(@CTR_DATE AS DATE)) AS DateMonthNum, 
					DATENAME(MONTH, CAST(@CTR_DATE AS DATE)) AS DateMonthName, 
					DAY(CAST(@CTR_DATE AS DATE)) AS DateDayNum, 
					DATENAME(DW, CAST(@CTR_DATE AS DATE)) AS DateDayName, 
					CASE DATEPART(DW, CAST(@CTR_DATE AS DATE)) 
						WHEN 1 THEN 1 
						WHEN 7 THEN 1 
						ELSE 0 END AS IsWeekend, 
					0 AS IsHoliday_HK, 
					0 AS IsHoliday_ID, 
					0 AS IsHoliday_JP, 
					0 AS IsHoliday_PH, 
					0 AS IsHoliday_SG, 
					0 AS IsHoliday_VN, 
					0 AS IsHoliday_CDN, 
					0 AS IsHoliday_USA				
			
			COMMIT TRAN 
			SET @ROLLBACK = 0
			
		END
		
		--  GET NEXT DATE:  --
		SET @CTR_DATE = DATEADD(DD, 1, @CTR_DATE)	  
		SET @CTR_DATE = CAST(@CTR_DATE AS VARCHAR(50))
	END
	
	--  DEBUGGER:  --
	SET @STATUS = 'End Loop: Last Next Date: ' + CAST(@CTR_DATE AS VARCHAR(50))

	SELECT DateYear, COUNT(*) AS Inserted_In_TMP 
	FROM #TMP_DIM_DATE 
	GROUP BY DateYear
	ORDER BY DateYear DESC

	--  INSERT INTO DIM_DATE FROM RAW TABLE:  --
	BEGIN TRAN
	SET @ROLLBACK = 1

		SET @STATUS = 'Begin Insert from RAW Table To DIM Table'

		INSERT INTO util_refined_db.dim_date 
		SELECT * 
		FROM #TMP_DIM_DATE

	COMMIT TRAN 
	SET @ROLLBACK = 0
	
	--  END LOGIC HERE:  --
	END_LOGIC:
	
	SET @STATUS = 'End Logic At: ' + CAST(@CTR_DATE AS VARCHAR(50))

	--  DROP RAW TABLE  --
	IF OBJECT_ID('TEMPDB..#TMP_DIM_DATE') IS NOT NULL 
	BEGIN
		DROP TABLE #TMP_DIM_DATE
	END 
	
	--  DEBUGGER:  --
	SELECT @STATUS AS STATUS, @CTR_DATE AS LAST_CTR_DATE, @END_DATE AS END_DATE, 
		COUNT(DateShort) AS Values_In_DIM, DateYear 
		FROM util_refined_db.dim_date
	GROUP BY DateYear
	ORDER BY DateYear DESC

END TRY

--  CATCH ERROR HERE:  --
BEGIN CATCH

	IF @ROLLBACK = 1 ROLLBACK TRAN

	--  ON ERROR, DISPLAY INFORMATION:  --
	SELECT @CTR_DATE AS CTR_DATE,
	@Status AS FAILED_WHERE,
	@@ERROR AS GEN_ERROR,
	ERROR_NUMBER() AS ErrorNumber,
	ERROR_SEVERITY() AS ErrorSeverity,
	ERROR_STATE() AS ErrorState,
	ERROR_LINE() AS ErrorLine,
	ERROR_PROCEDURE() AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage

	--  DROP RAW TABLE  --
	IF OBJECT_ID('TEMPDB..#TMP_DIM_DATE') IS NOT NULL 
	BEGIN
		DROP TABLE #TMP_DIM_DATE
	END 
	
END CATCH
SET NOCOUNT OFF
END


GO
""")

# Output transformed dataframe
display(df_dim_date)
df_dim_date.printSchema()

# Save the transformed dataframe to delta table
df_dim_date.write.option("path", f"abfss://refined@gdmsutiladls.dfs.core.windows.net/util_refined_db/dim_date/")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"util_refined_db.dim_date")

# COMMAND ----------



# COMMAND ----------


