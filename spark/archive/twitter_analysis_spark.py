from __future__ import print_function

import sys
import re
from operator import add 
import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import json
import boto
import boto3
from boto.s3.key import Key
import boto.s3.connection
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def main():
    conf = SparkConf().setAppName("first")
    #sc = SparkContext(conf=conf)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",'xxx')
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",'xxx')
    config_dict = {"fs.s3n.awsAccessKeyId":"xxx",
               "fs.s3n.awsSecretAccessKey":"xxx"}
    bucket = "project4capstones3"
    prefix = "/2017/06/16//14/project*"
    filename = "s3n://{}/{}".format(bucket, prefix)
    warehouse_location = 'spark-warehouse'
    rdd = sc.hadoopFile(filename,
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.io.Text',
            'org.apache.hadoop.io.LongWritable',
            conf=config_dict)
    spark_sess = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    df = spark_sess.read.json(rdd.map(lambda x: x[1]))
    data_rm_na = df.filter(df['status_id']!='None')
    features_of_interest = ['user_followers_count', 'user_friends_count', 'user_statuses_count', 'rt_status_retweet_count', 'rt_status_favorite_count',
                        'rt_status_entities_user_mentions','user_listed_count', 'status_num_mentions', 'status_retweet_count', 'status_geo',
                        'status_hashtags','user_name', 'user_loc', 'status_text', 'user_desc', 'user_img_url', 'user_id', 'searched_names', 
                        'status_sentMag', 'status_sentScore']
    df_reduce= data_rm_na.select(features_of_interest)
df_reduce = df_reduce.withColumn("user_followers_count", df_reduce["user_followers_count"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("user_friends_count", df_reduce["user_friends_count"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("user_statuses_count", df_reduce["user_statuses_count"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("rt_status_retweet_count", df_reduce["rt_status_retweet_count"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("user_listed_count", df_reduce["user_listed_count"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("status_num_mentions", df_reduce["status_num_mentions"].cast(IntegerType()))
df_reduce = df_reduce.withColumn("status_retweet_count", df_reduce["status_retweet_count"].cast(IntegerType()))
# df_reduce = df_reduce.withColumn(“status_sentMag”, df_reduce[“status_sentMag”].cast(DoubleType()))
# df_reduce = df_reduce.withColumn(“status_sentScore”, df_reduce[“status_sentScore”].cast(DoubleType()))
df_reduce = df_reduce.withColumn("rt_status_favorite_count", df_reduce["rt_status_favorite_count"].cast(IntegerType()))

df_writer = pyspark.sql.DataFrameWriter(df_reduce)
df_writer.saveAsTable('test_table', mode='overwrite')
    



if __name__ == "__main__":
    main()