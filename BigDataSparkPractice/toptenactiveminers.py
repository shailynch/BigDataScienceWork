#Top 10 most active miners
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
    
    #Function for Cleaning Blocks 
    def good_line(line):
        try: 
            field = line.split(",")
            if(len(field)==19):
                field = line.split(",")
                float(field[12])
                return True
            else:
                return False
        except:
            return False
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    
    clean_blocks = blocks.filter(good_line)
    
    features = clean_blocks.map(lambda x: (x.split(",")[9], float(x.split(",")[12])))
    
    result = features.reduceByKey(lambda a,b: a+b)
    
    top_ten = result.takeOrdered(10,key=lambda x: -x[1])
    
    rdd = spark.sparkContext.parallelize(top_ten)
    
    data_collected = rdd.coalesce(1)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    data_collected.saveAsTextFile("s3a://" + s3_bucket + "/top_ten_miners_" + date_time)
    
    spark.stop()