import json

def func_create_external_delta_table(spark_context, str_db_name, str_tbl_name, str_delta_path):
    spark_context.sql(f"CREATE DATABASE IF NOT EXISTS {str_db_name};")
    spark_context.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS {str_db_name}.{str_tbl_name} USING DELTA LOCATION '{str_delta_path}' ")
    spark_context.sql(f"REFRESH TABLE {str_db_name}.{str_tbl_name}")

def func_get_config(str_config_path):
    with  open(f"{str_config_path}","r") as f:
        return json.load(f)

def func_get_latest_path_from_date_hierarchy(str_path, dbutil):
    str_year_max = max([x.name for x in dbutil.fs.ls(str_path)])
    str_month_max = max([x.name for x in dbutil.fs.ls(str_path + str_year_max)])
    str_day_max = max([x.name for x in dbutil.fs.ls(str_path + str_year_max + str_month_max)])
    str_hr_min_max = max([x.name for x in dbutil.fs.ls(str_path + str_year_max + str_month_max + str_day_max)])

    return str_path + str_year_max + str_month_max + str_day_max + str_hr_min_max
