import boto3
import csv
from io import StringIO
from decimal import Decimal

# Initialize S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('table1')

def lambda_handler(event, context):
    # Specify the bucket name and object key
    bucket_name = 'finaltaskbucket2'
    object_key = 'uploaded_file.csv'
    
    # Read the CSV file from S3
    csv_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    csv_content = csv_object['Body'].read().decode('utf-8')
    
    # Parse the CSV content
    csv_data = csv.DictReader(StringIO(csv_content))
    records_count = 0
    data_types = {}
    
    for row in csv_data:
        records_count += 1
        for key, value in row.items():
            if key not in data_types:
                data_types[key] = set()
            if value:
                try:
                    float(value)
                    data_types[key].add('number')
                except ValueError:
                    data_types[key].add('string')
    
    # Store the data in DynamoDB
    item = {
        'ID': 'csv_file_info',
        'RecordsCount': Decimal(records_count),
        'DataTypes': {k: list(v) for k, v in data_types.items()}
    }
    table.put_item(Item=item)
    
    return {
        'statusCode': 200,
        'body': 'CSV file processed successfully and data stored in DynamoDB.'
    }
