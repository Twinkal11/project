import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import re


class raw_ly:
    spark = SparkSession.builder.appName("raw_layer") \
        .config('spark.ui.port', '4050').config("spark.master", "local") \
        .enableHiveSupport().getOrCreate()



    def read_csv_file(self):
        spark = SparkSession.builder.appName("raw_layer") \
            .config('spark.ui.port', '4050').config("spark.master", "local") \
            .enableHiveSupport().getOrCreate()
        print("Read Data Frame")
        df=self.spark.read.text("D:/PROJECT_DATA/script_file/project-demo-processed-input.txt")



        # Building regular expression sets for lookup

        host = r'(^\S+\.[\S+\.]+\S+)\s'
        time_stamp = r'(\d+/\w+/\d+[:\d]+)'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        status = r'\s(\d{3})\s'
        content_size_pattern = r'\s(\d{4,5})\s'
        referer_pattern = r'("https(\S+)")'
        # user_pattern=r'"(Mozilla|Dal)(\S+\s\S+)+"'
        useragent_pattern = r'(Mozilla|Dalvik)(\S+\s+)*'
        user_device_pattern = r'(Mozilla|Dal|Goog|troob|bar)\S*\s\((\w+;?\s+\w+)'

        # Regular-exact-->Return Exact a Specific Group matched, from the specific column

        self.logs_df = df.withColumn("Row_id", monotonically_increasing_id()) \
            .select("Row_id", regexp_extract('value', host, 1).alias('host'),
                    regexp_extract('value', time_stamp, 1).alias('timestamp'),
                    regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                    regexp_extract('value', method_uri_protocol_pattern, 2).alias('request'),
                    regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                    regexp_extract('value', status, 1).cast('integer').alias('status'),
                    regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'),
                    regexp_extract('value', referer_pattern, 1).alias('referer'),
                    regexp_extract('value', useragent_pattern, 0).alias('User_agent'),
                    regexp_extract('value', user_device_pattern, 2).alias('User_device')
                    )

        self.logs_df.show()

        return self.logs_df

    # def save(self):
    #     self.logs_df.write.mode("overwrite").format("csv").save("D:\result_data\raw_layer\raw_layer.csv")


    def clean(self):
        spe_char = r'[%,|"-&.=?-]'

        self.clean_df = self.logs_df.withColumn("timestamp", to_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss")) \
            .withColumn("request", regexp_replace("request", spe_char, "")) \
            .withColumn("size", round(col("content_size") / 1024, 2)) \
            .withColumn('referer_present', when(col('referer') == '', 'N') \
                        .otherwise('Y'))

    def clean_lyr(self):
        self.clean_df = self.clean_df.withColumn("date", split(self.clean_df["timestamp"], ' ') \
                                          .getItem(0)).withColumn("datetimestamp",
                                                                  to_timestamp("timestamp", 'dd/MMM/yyyy:hh:mm:ss')) \
            .withColumn("datetimestamp", to_timestamp("timestamp", 'MMM/dd/yyyy:hh:mm:ss'))
        print("Schema")
        self.clean_df.printSchema()

        self.clean_df.na.fill("NA")

        print("remove referer column")
        self.curated = self.clean_df.drop('referer')
        self.curated.show(truncate=False)
        #
        # count of perticular method
        # # With sorting.
        print("Count of Method")
        self.curated.groupBy("method").count().orderBy(desc("count")).show()

        self.curated.select(count(self.clean_df.method)).show()

        print("adding Hour Column")
        curated_data=self.curated.withColumn('Hour', hour('timestamp'))
        #
        # self.curated.show()

        # With sorting.
        print("count of host")
        curated_data.groupBy("host").count().orderBy(desc("count")).show()

        curated_data.groupBy("User_device", "Hour").count().orderBy("count").show()

        print("Count of User_device")
        curated_data.groupBy("User_device").count().show()

        curated_data.select(["host", "User_device"]).show(10)

        device_pattern = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
        self.device_curated=curated_data.withColumn('device', regexp_extract(col('User_agent'), device_pattern, 2))
        self.device_curated.show()

        # # add column hour,Get,Post,Head
    def curated_l(self):
        print("add column hour,Get,Post,Head")
        self.curated_lyr =self.device_curated.withColumn("No_get", when(col("method") == "GET", "GET")) \
            .withColumn("No_post", when(col("method") == "POST", "POST")) \
            .withColumn("No_Head", when(col("method") == "HEAD", "HEAD"))
        self.curated_lyr.show(500)

    def per_device(self):
        print("agg_per_ip")
        # # perform aggregation per device
        self.agg_per_ip = self.curated_lyr.select("*").groupBy("host").agg(
            count("Row_id").alias("row_id"), sum("Hour").alias("day_hour"), count("host").alias("count_client/ip"),
            count(col("No_get")).alias("no_get"), count(col("No_post")).alias("no_post"),
            count(col("No_head")).alias("no_head"))

        self.agg_per_ip.show()

        print("per_device_result")
        self.per_device_result = self.curated_lyr.select("*").groupBy("User_device").agg(count("Row_id").alias("Row_id"), \
                                                                               sum("Hour").alias("Hour"),
                                                                               count("host").alias("Client/IP"), \
                                                                               count(col("No_get")).alias("NO_GET"),
                                                                               count(col("No_post")).alias("NO_POST"), \
                                                                               count(col("No_Head")).alias("NO_HEAD"))
        #
        self.per_device_result.show()

    def across_device(self):
        print("agg_across_device")
        agg_across_device = self.curated_lyr.select("*").agg(count("Row_id").alias("row_id"),
                                                    first("hour").alias("day_hour"),
                                                    count("host").alias("count_client/ip"),
                                                    count(col("No_get")).alias("no_get"),
                                                    count(col("No_post")).alias("no_post"),
                                                    count(col("No_head")).alias("no_head"))

        agg_across_device.show()

    # def write_to_hive(self):
    #     self.device_curated.write.saveAsTable('device_curated')




    #     #
if __name__ == '__main__':
    raw = raw_ly()
    raw.read_csv_file()
    raw.clean()
    raw.clean_lyr()
    raw.curated_l()
    raw.per_device()
    raw.across_device()
    # raw.write_to_hive()
        
        
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
        .option("dbtable", "curated_table").mode("overwrite")\
        .save()


main()


