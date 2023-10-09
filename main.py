#!/usr/bin/env python3

##main file for datapipelinedemo script


from pyspark.sql import SparkSession
from pyspark.sql.functions import when 
import boto3
import os
import json

def main():
    # Creating a Spark session so we can use Spark

    aws_config_file = os.path.expanduser('~/aws_config.json')
    
    with open(aws_config_file, 'r') as config_file:
        aws_config = json.load(config_file)

    aws_access_key_id = aws_config['aws_access_key_id']
    aws_secret_access_key = aws_config['aws_secret_access_key']
    s3_bucket = aws_config['s3_bucket']
    s3_path = aws_config['s3_path']

    spark = SparkSession.builder \
    .appName("domemadeDataPipe") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.multipart.purge", "true") \
    .getOrCreate()

    # Suppress warning messages unrelated to the purpose of this exercise
    spark.sparkContext.setLogLevel("ERROR")

    # Finding a nice dataset!

    # What is a spark dataframe? https://spark.apache.org/docs/latest/sql-programming-guide.html
    # Header parameter - whether the first col of your csv are the column names, in this case, yes, set to true
    # InferSchema - should pyspark figure out the data types of your cols for your table schema
    df = spark.read.csv('fighters_index.csv', header=True, inferSchema=True)
    # df.show()

    # lets clean this data up a little bit - lets call this preprocessing raw data
    df = df.withColumn("age", when(df["age"] == "Unknown", None).otherwise(df["age"]))
    df = df.withColumn("height", when(df["height"] == "Unknown", None).otherwise(df["height"]))
    df = df.withColumn("reach", when(df["reach"] == "Unknown", None).otherwise(df["reach"]))

    # df.show()

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    df.write.mode("overwrite").parquet(f's3a://{s3_bucket}/{s3_path}')
    #print('i am here!!')

    # Stopping the Spark session
    spark.stop()



main()

