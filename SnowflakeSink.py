import random
import pandas as pd
import datetime
from pyspark.sql import SparkSession, functions, Row, types
import numpy as np

#define variables required for file location of csv file containing the data for processing...
file_location = "dbfs:/FileStore/shared_uploads/mnagargoje@integralads.com/snowflake_sample_raw5.csv"
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimeter = ","
    
#Read data from csv file....
df = spark.read\
    .format(file_type)\
    .option("inferSchema", infer_schema)\
    .option("header", first_row_is_header)\
    .option("sep", delimeter)\
    .load(file_location)\
    .withColumn("current_timestamp", functions.unix_timestamp())

user = dbutils.secrets.get("data-warehouse", "snowflake-user")
password = dbutils.secrets.get("data-warehouse", "snowflake-password")
database_host_url = "ias.us-east-1.snowflakecomputing.com"
database_name = "CDS_PROD"
schema_name = "LOOKER_SCRATCH_DEV"
warehouse_name = "ADHOC_WH"

temp_table_gaming_agg_reports = df.select("HIT_DATE", #Date
                                                  "CAMPAIGN_ID", #number
                                                  "PUBLISHER_ID", #number
                                                  "PLACEMENT_ID", #number
                                                  "MEDIA_TYPE_ID", #number
                                                  "AVERAGE_IN_VIEW_TIME", #float
                                                  "IN_VIEW_PASSED_IMPS", #number
                                                  "NOT_IN_VIEW_PASSED_IMPS", #number
                                                  "MEASUREMENT_SOURCE_TYPE",
                                                  "TEAM_ID"
#                                                   "GROSS_IAS_IMPS",
#                                                   "GROSS_PM_IMPS",
#                                                   "GROSS_PM_YT_BS_IMPS",
#                                                   "PM_YT_BS_VIEWABILITY_COMMON_GIVT_IMPS",
#                                                   "GROSS_PM_YT_IMPS"
                                         )

gaming_agg_reports_ad_details = temp_table_gaming_agg_reports.write \
  .format("snowflake") \
  .option("sfUrl", database_host_url) \
  .option("column_mismatch_behavior","ignore") \
  .option("sfUser", user) \
  .option("sfPassword", password) \
  .option("sfDatabase", database_name) \
  .option("sfSchema", schema_name) \
  .option("sfRole", "ADHOC_CONSUMER_PROD") \
  .option("column_mapping", "name") \
  .option("sfWarehouse", warehouse_name) \
  .mode("append") \
  .option("dbtable", "AGG_AGENCY_CUSTOM_DAILY") \
  .save()
