from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
        .builder \
        .appName("DemoJob") \
        .getOrCreate()

def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "TWINKAL_DEMO"
    snowflake_schema = "PUBLIC"
    snowflake_options = {
    "sfUrl":"https://app.snowflake.com/ap-south-1.aws/wq71446/w26bJ0UWlpUE#query",
    "sfUser": "twinkaldesai",
    "sfPassword": "Twinkal@123",
    "sfDatabase": "TWINKAL_DATA",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "DEMO"
    }
    df = spark.read\
        .format('parquet').load('s3://twinkal11/project_Raw_data/curated_layer/curatelayer.csv/part-00000-65844081-0a85-4d6d-94d6-17a061cb03ba-c000.csv')
    df1 = df.select("Row_id","No_Head","No_post","No_get","host","timestamp","method","request","status","content_size","new_refer","User_agent")
    df1.write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "curated_layer").mode("overwrite")\
        .save()


main()
