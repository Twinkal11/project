import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession


spark = SparkSession.builder\
        .master("local")\
        .appName("demo")\
        .enableHiveSupport()\
        .config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3')\
        .getOrCreate()

sfOptions = {
  "sfURL" : "https://app.snowflake.com/ap-south-1.aws/hh82548/worksheets",
  "sfUser" : "twinkaldesai",
  "sfPassword" : "twinkal@123",
  "sfDatabase" : "twinkal_DATA",
  "sfSchema" : "CURATED_DATA",
  "sfWarehouse" :"COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


import re
#read data from local
df=spark.read.text("s3://twinkal11/project_Raw_data/log_data_ip_request.txt")

df.printSchema()

print((df.count(),len(df.columns)))

type(df)

rdd_df=df.rdd
type(rdd_df)

df.show(10,truncate=False)

#RAW LAYER

# Extract and take a look at Sample Logs
sample_logs = [x['value'] for x in df.take(15)]

# Building regular expression sets for lookup

host = r'(^\S+\.[\S+\.]+\S+)\s'
time_stamp = r'(\d+/\w+/\d+[:\d]+)'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)\s"'
referer_pattern=r'("https(\S+)")'
# user_pattern=r'"(Mozilla|Dal)(\S+\s\S+)+"'
useragent_pattern = r'(Mozilla|Dalvik)(\S+\s+)*'
user_device_pattern = r'(Mozilla|Dal|Goog|troob|bar)\S*\s\((\w+;?\s+\w+)'

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import regexp_extract

#Regular-exact-->Return Exact a Specific Group matched, from the specific column

logs_df = df.withColumn("Row_id",monotonically_increasing_id())\
            .select("Row_id",regexp_extract('value', host, 1).alias('host'),
             regexp_extract('value', time_stamp, 1).alias('timestamp'),
             regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
             regexp_extract('value', method_uri_protocol_pattern, 2).alias('request'),
             regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
             regexp_extract('value', status, 1).cast('integer').alias('status'),
             regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'),
             regexp_extract('value',referer_pattern,1).alias('referer'),
             regexp_extract('value',useragent_pattern,0).alias('User_agent'),
             regexp_extract('value',user_device_pattern,2).alias('User_device')
             )


logs_df.show(20, truncate=True)
print((logs_df.count(), len(logs_df.columns)))


#change data type using to_timestamp function
from pyspark.sql.functions import *
df1=logs_df.withColumn("timestamp",to_timestamp("timestamp","dd/MMM/yyyy:HH:mm:ss"))

df1.show()
df1.printSchema()

#status for referer , if referer prsent it will print "Y",Otherwise it will print"N"
from pyspark.sql.functions import when
New_refer = df1.withColumn("new_refer", when(df1.referer == "","N").otherwise("Y"))
New_refer.show(5)

New_refer.write.csv("s3://twinkal11/project_Raw_data/clean_layer/clean_layer.csv",mode="overwrite")

df1.filter( (df1.referer  == "") | (df1.referer  == df1.referer) ) \
    .show(5,truncate=True)

#Drop referer field
clean=New_refer.drop("referer")

clean.show(6)

clean.write.csv("s3://twinkal11/project_Raw_data/curated_layer/curatelayer.csv",mode="overwrite")

#AGGREGATION

#finding distinct value of method
Dist=df1.select("method").distinct().show()

code=df1.select("status").distinct().show()

code_count=df1.groupBy("status").count().show()

#count of perticular method
from pyspark.sql.functions import desc
# With sorting.
ord=clean.groupBy("method").count().orderBy(desc("count")).show()

df1.select(count(df1.method)).show()

#Finding Null values for all fields (using isNULL() functions)
df2 = df1.filter(df1['host'].isNull()|
                             df1['timestamp'].isNull() |
                             df1['method'].isNull() |
                             df1['request'].isNull() |
                             df1['status'].isNull() |
                             df1['content_size'].isNull()|
                             df1['protocol'].isNull())
df2.count()

#create New column for filtering hour
from pyspark.sql.functions import hour
hour_wise=clean.withColumn('Hour', hour(clean.timestamp))

date=hour_wise.withColumn("Date",col("timestamp").cast('date'))
# date.show(4)
#GRoupBy on Host field
from pyspark.sql.functions import desc
# With sorting.
client_IP=date.groupBy("host").count().orderBy(desc("count")).show()

#FINDING DISTINCT COUNT ON HOST
from pyspark.sql.functions import countDistinct

host1 = date.groupBy("host").agg(countDistinct("host")).show()


#Grouping of User_device and Hour
Device_Hour= date.groupBy("User_device","Hour").count().orderBy("count").show(20)

date.groupBy("User_device").count().show()

#COUNT OF HOUR
hour=date.groupBy("Hour").count().show()

curated=date.withColumn("No_get",when(col("method")=="GET","GET"))\
            .withColumn("No_post",when(col("method")=="POST","POST"))\
            .withColumn("No_Head",when(col("method")=="HEAD","HEAD"))

curated.show(10)

curated.select(["host","User_device"]).show(4)

log_agg_per_device=curated.drop("timestamp","method","request","protocol","status","content_size","User_agent")

log_agg_per_device.show(6)

from pyspark.sql.functions import *
from pyspark.sql.types import *

per_device = clean.filter(col("User_agent") != "").withColumn("day_hour", hour(col("timestamp"))) \
    .groupBy("User_agent") \
    .agg(count(col("Row_id")).alias("row_id"), \
         first("day_hour").alias("day_hour"), \
         count(col("host")).alias("client/ip")

         )

per_device.show()

use_func = lambda x:sum(when(x,1).otherwise(0))

#Per device aggregation
per_device = clean.filter(col("User_agent")!="").withColumn("day_hour", hour(col("timestamp"))) \
             .groupBy("User_agent") \
             .agg(count(col("Row_id")).alias("row_id"),
             first("day_hour").alias("day_hour"),
             count(col("host")).alias("client/ip"),
             use_func(col("method")=="GET").alias("no_GET"),
             use_func(col("method")== "POST").alias("no_post"),
             use_func(col("method") == "HEAD").alias("no_head"),
             )

per_device.show()

per_device_11 = clean.filter(col("User_agent")=="").withColumn("day_hour", hour(col("timestamp"))) \
             .groupBy("User_agent") \
             .agg(count(col("Row_id")).alias("row_id"),
             first("day_hour").alias("day_hour"),
             count(col("host")).alias("client/ip"),
             use_func(col("method")=="GET").alias("no_GET"),
             use_func(col("method")== "POST").alias("no_post"),
             use_func(col("method") == "HEAD").alias("no_head"),
             ).show()



per_device_result=curated.select("*").groupBy("User_device").agg(count("Row_id").alias("Row_id"),
                          sum("Hour").alias("Hour"),count("host").alias("Client/IP"),
                          count(col("No_get")).alias("NO_GET"),count(col("No_post")).alias("NO_POST"),
                          count(col("No_Head")).alias("NO_HEAD"))

per_device_result.show()

per_device.write.csv("s3://twinkal11/project_Raw_data/per_device/perdevice.csv",mode="overwrite")
per_device_result.write.csv("s3://twinkal11/project_Raw_data/per_dev_res_layer/per_device_res.csv",mode="overwrite")


#Across Device Aggregation
Across_device_result=curated.select("*").agg(count("Row_id").alias("Row_id"),
                                             sum("Hour").alias("hour"),
                                             count("host").alias("client/IP"),
                                             count(col("No_get")).alias("NO_GET"),
                                             count(col("No_post")).alias("NO_POST"),
                                             count(col("No_Head")).alias("NO_HEAD"))


Across_device_result.show()

Across_device_result.write.csv("s3://twinkal11/project_Raw_data/Across_layer/across_device.csv",mode="overwrite")

Across_device_result.write.mode("overwrite").saveAsTable("log_agg_across_device")
per_device.write.mode('overwrite').saveAsTable("per_device")
