from pyspark.sql import *
from pyspark.sql.functions import *


def write_to_snowflake():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "twinkaldb"
    snowflake_schema = "public"
    target_table_name = "curated_log_details"
    snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "sushantsangle",
        "sfPassword": "Stanford@01",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
    spark = SparkSession.builder \
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    df_raw = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//Raw_log_twinkal.csv")
    raw = df_raw.select("*")
    raw.write.format("snowflake")\
        .options(**snowflake_options) \
        .option("dbtable", "raw_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_cleans = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//clean_log_twinkal.csv")
    cleans = df_cleans.select("*")
    cleans.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "cleans_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_curate = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//curated_log_twinkal.csv")
    curate = df_curate.select("*")
    curate.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curate_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_per = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//agg_per_ip_twinkal.csv")
    per = df_per.select("*")
    per.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_per_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_across = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//agg_across_ip_twinkal.csv")
    across = df_across.select("*")
    across.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_across_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

write_to_snowflake()


