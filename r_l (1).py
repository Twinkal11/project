# -*- coding: utf-8 -*-
"""r_l.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1VR2LBWaNfPbNq2LjGyeVJnHVwTaMyJKH
"""

!pip3 install pyspark

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import re



class raw_ly:
  spark = SparkSession.builder.appName("raw_layer") \
        .config('spark.ui.port', '4050').config("spark.master", "local") \
        .enableHiveSupport().getOrCreate()
  def read_csv_file(self):
    spark = SparkSession.builder.appName("raw_layer") \
            .config('spark.ui.port', '4050').config("spark.master", "local") \
            .enableHiveSupport().getOrCreate()
    df=self.spark.read.text("/content/drive/MyDrive/project-demo-processed-input.txt")

    host = r'(^\S+\.[\S+\.]+\S+)\s'
    time_stamp = r'(\d+/\w+/\d+[:\d]+)'
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    status = r'\s(\d{3})\s'
    content_size_pattern = r'\s(\d{4,5})\s'
    referer_pattern = r'("https(\S+)")'
   
    # user_device_pattern = r'(Mozilla|Dal|Goog|troob|bar)\S*\s\((\w+;?\s+\w+)'


    self.logs_df = df.withColumn("Row_id", monotonically_increasing_id()) \
            .select("Row_id", regexp_extract('value', host, 1).alias('client/ip'),
                    regexp_extract('value', time_stamp, 1).alias('timestamp'),
                    regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                    regexp_extract('value', method_uri_protocol_pattern, 2).alias('request'),
                    # regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                    regexp_extract('value', status, 1).cast('integer').alias('status'),
                    regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'),
                    regexp_extract('value', referer_pattern, 1).alias('referer')
                    )
    self.logs_df.show()

  def save(self):

    self.logs_df.write.mode("overwrite").format("csv").save("/content/drive/MyDrive/raw_layer/raw_l.csv",header="True",mode='overwrite')

  def show_On_Hive(self):
    pass
    self.logs_df.write.option("mode","overwrite").saveAsTable('Raw_D')
    self.spark.sql("select count(*) from Raw_D").show()


if __name__ == '__main__':
  raw = raw_ly()
  raw.read_csv_file()
  raw.save()
  raw.show_On_Hive()

