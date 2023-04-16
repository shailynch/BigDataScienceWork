import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

APP_NAME = "Scam_Analysis"
TRANSACTION_FILE_PATH = "/ECS765/ethereum-parvulus/transactions.csv"
SCAM_FILE_PATH = "/ECS765/ethereum-parvulus/scams.csv"

def good_line_tran(line):
    try:
        fields = line.split(',')
        if len(fields) != 15:
            return False
        int(fields[11]) # block_timestamp
        str(fields[6]) # to address
        float(fields[7]) # value
        return True
    except:
        return False

def good_line_scam(line):
    try:
        fields = line.split(',')
        if len(fields) != 8:
            return False
        str(fields[6]) # addresses
        str(fields[4]) # category
        str(fields[7]) # status
        return True
    except:
        return False
    
def mapper_transactions(line):
    fields = line.split(',')
    to_address = str(fields[6])
    value = int(fields[7])
    block_timestamp = int(fields[11])
    month_year = time.strftime('%m-%Y', time.gmtime(block_timestamp))

    return (to_address, (month_year, value))
    
def scam_analysis():
    spark = SparkSession\
        .builder\
        .appName(APP_NAME)\
        .getOrCreate()
    
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
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + TRANSACTION_FILE_PATH)
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + SCAM_FILE_PATH)
    
    trans_clean_line = transactions.filter(good_line_tran)
    scams_clean_line = scams.filter(good_line_scam)
    

    lucrative_scams_features = scams_clean_line.map(lambda l: (l.split(',')[6], (l.split(',')[0],l.split(',')[4])))
    lucrative_trans_feature = trans_clean_line.map(lambda l:  (l.split(',')[6], float(l.split(',')[7])))
    lucrative_trans_and_scams = lucrative_trans_feature.join(lucrative_scams_features)
    # ((id, category), value)
    lucrative_scams = lucrative_trans_and_scams.map(lambda x: ((x[1][1][0], x[1][1][1]), float(x[1][0])))
    lucrative_scams = lucrative_scams.reduceByKey(operator.add)
    lucrative_scams = lucrative_scams.takeOrdered(10, key=lambda x: -x[1])
    print(lucrative_scams)
    

    time_scams_feature = scams_clean_line.map(lambda l:(l.split(',')[6], l.split(',')[4]))
    time_trans_feature = trans_clean_line.map(mapper_transactions)
    time_trans_and_scams = time_trans_feature.join(time_scams_feature)
    # ((date, category), value)
    time_scams = time_trans_and_scams.map(lambda x: ((x[1][0][0], x[1][1]), float(x[1][0][1]))) 
    time_scams = time_scams.reduceByKey(operator.add)
    print(time_scams.take(100)) # only 46 items
   
    # save the result
    my_bucket_resource = boto3.resource('s3',
             endpoint_url='http://' + s3_endpoint_url,
             aws_access_key_id=s3_access_key_id,
             aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/lucrative_scam.txt')
    my_result_object.put(Body=json.dumps(lucrative_scams))
    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/time_scams.txt')
    my_result_object.put(Body=json.dumps(time_scams.take(100)))

if __name__ == "__main__":
    print("start partD - Scam")
    scam_analysis()
    print("the end")