import boto3
import botocore
import os

def upload_file(filename,bucket_name):
    s3 = boto3.client('s3')
    s3.upload_file("/tmp/"+filename, bucket_name, filename)

def download_file(key,bucket_name):
    try:
        s3 = boto3.client('s3')
        #Posem el mateix nom a l'arxiu descarregat
        s3.download_file(bucket_name,key,"/tmp/"+key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
