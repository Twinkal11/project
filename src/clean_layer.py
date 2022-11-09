# -*- coding: utf-8 -*-
"""clean_l.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/14M5mwJ_mRjodxOkR2t-y6myr2Qnip2pz
"""

!pip3 install pyspark

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import re
from pyspark.sql import HiveContext
from pyspark.sql.functions import desc

class clean_layer():
  spark = SparkSession.builder.appName("Project-Stage-II").config('spark.ui.port', '4050')\
        .config("spark.master", "local").enableHiveSupport().getOrCreate()
  clean_df = spark.read.csv("/content/drive/MyDrive/raw_layer/raw_l.csv/",header="True")
  
  # clean_df.show()
 
  
  def clean(self):
    
        
    self.clean_df=self.clean_df.na.fill("NA")
#withColumn("timestamp", to_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss")) \
    self.clean_df=self.clean_df.withColumn("Datetime", to_timestamp("Datetime", "dd/MMM/yyyy:HH:mm:ss"))\
           .withColumn("content_size", round(col("content_size") / 1024)) \
           .withColumnRenamed("content_size","Size_kb")\
            .withColumn('referer_present',when(col('referer') == "NA", "N") \
                        .otherwise('Y'))
            

           
  # def clean_lyr(self):
  #   # self.clean_df = self.clean_df = self.clean_df.withColumn("Datetime", split(self.clean_df["Datetime"], ' ')\
  #                                                           #  .getItem(0)).withColumn("Datetime",to_timestamp("Datetime",'dd/MMM/yyyy:HH:mm:ss')) 
  #           # .withColumn("datetimestamp", to_timestamp("timestamp", 'MMM/dd/yyyy:hh:mm:ss'))



    # self.clean_df.printSchema()

   

    self.clean_df=self.clean_df.drop(col('referer'))\
                        .na.drop()
    self.clean_df.show(100)



  def clean_save(self):
    self.clean_df.write.csv("/content/drive/MyDrive/clean_layer/clean_layer.csv", header=True,mode='overwrite')

  def hive_table_for_clean(self):
    sqlContext = HiveContext(self.spark.sparkContext)
    sqlContext.sql('DROP TABLE IF EXISTS clean_lyr1')
    self.clean_df.write.option("mode","overwrite").saveAsTable('clean_lyr1')
    self.spark.sql("select count(*) from clean_lyr1").show()


if __name__ == '__main__':
    clean_l = clean_layer()
    clean_l.clean()
    # clean_l.clean_lyr()
    clean_l.clean_save()
    clean_l.hive_table_for_clean()
