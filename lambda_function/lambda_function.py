import json
import boto3
import os
import csv
from datetime import datetime

s3_client = boto3.client('s3', endpoint_url="http://localhost:4566")
dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:4566")

TABLE_NAME = "csv_metadata"

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        
        # Download file
        temp_file = f"/tmp/{file_key}"
        s3_client.download_file(bucket_name, file_key, temp_file)
        
        # Extract metadata
        metadata = extract_metadata(temp_file, file_key)
        
        # Store metadata in DynamoDB
        store_metadata(metadata)
        
        return {
            "statusCode": 200,
            "body": json.dumps("Metadata extracted successfully")
        }

def extract_metadata(file_path, filename):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        row_count = sum(1 for _ in reader)
    
    file_size = os.path.getsize(file_path)
    upload_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    metadata = {
        "filename": filename,
        "upload_timestamp": upload_timestamp,
        "file_size_bytes": file_size,
        "row_count": row_count,
        "column_count": len(headers),
        "column_names": headers
    }
    
    return metadata

def store_metadata(metadata):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=metadata)
