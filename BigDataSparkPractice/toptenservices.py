#Top 10 services
#import required libraries
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
            #Transaction CSV
            if (len(field) == 15):
                field = line.split(",")
                #Value
                float(field[7])
                return True
            #Contract CSV
            elif(len(field) == 6):
                field = line.split(",")
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
    
    #Blocks
    contract_blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    #Cleaned Contract Blocks
    contract_good_blocks = contract_blocks.filter(good_line)
    
    transaction_blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    #Cleaned Transaction block
    transaction_good_blocks = transaction_blocks.filter(good_line)
    
    #Features 
    contract_features = contract_good_blocks.map(lambda x:(x.split(",")[0],1))
    
    transaction_features = transaction_good_blocks.map(lambda x:(x.split(",")[6],float(x.split(",")[7])))
    
#     #Tables 
    transaction_table = transaction_features.reduceByKey(lambda x,y: (x+y))
    
    contract_table = contract_features
    
#     #filter and Join the Tables 
    t_c_joined = contract_table.leftOuterJoin(transaction_table)
    filtered_table = t_c_joined.filter(lambda x: None not in x[1])
    
    final_table = filtered_table.map(lambda x: (x[0],x[1][1]))
    
#get top ten
    top_10 = final_table.takeOrdered(10, key= lambda x: -x[1])
    
    #Create a RDD from the spark
    rdd = spark.sparkContext.parallelize(top_10)
    
    #Reduce the number of partitions putting the top 10 into 1 array
    data_collected = rdd.coalesce(1)
    
        
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    data_collected.saveAsTextFile("s3a://" + s3_bucket + "/top_ten_services_" + date_time)
    
    spark.stop