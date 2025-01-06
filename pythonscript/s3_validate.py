import boto3
from botocore.exceptions import ClientError
import os

import pandas as pd
import json
import logging

aws_storage_options = {
    "key" : os.getenv('AWS_ACCESS_KEY_ID'),
    "secret" : os.getenv('AWS_SECRET_ACCESS_KEY')
}

# connection to s3
def connect_s3():
    s3_client = boto3.client(
        's3', 
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='ap-northeast-2'
    )
    return s3_client

# check if file exists in s3
def check_file_exists(s3_client, bucket_name, s3_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(e)
            return False

# upload json to s3
def upload_json_to_s3(s3_client, bucket_name, s3_key, json_data):
    try:
        json_string = json.dumps(json_data)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_string)
        logging.info(f"File {s3_key} uploaded to {bucket_name}.")
    except ClientError as e:
        logging.error(f"Error uploading file: {e}")

# validate s3 file and delete if exists
def validate_and_upload_s3_file(s3_client, bucket_name, s3_key, json_data):
    if check_file_exists(s3_client, bucket_name, s3_key):
        logging.info(f'{s3_key} already exists in {bucket_name}')
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        logging.info(f"File {s3_key} deleted successfully.")
        upload_json_to_s3(s3_client, bucket_name, s3_key, json_data)
        logging.info(f"File {s3_key} uploaded successfully.")
    else:
        logging.info(f'{s3_key} does not exist in {bucket_name}')
        upload_json_to_s3(s3_client, bucket_name, s3_key, json_data)
        logging.info(f"File {s3_key} uploaded successfully.")

# product_id 추출
def get_product_ids(bucket_path, file_key):
    file_path = bucket_path + file_key
    df = pd.read_parquet(file_path, storage_options=aws_storage_options)
    return df['product_id'].tolist()