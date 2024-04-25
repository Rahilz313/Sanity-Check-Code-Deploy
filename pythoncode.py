import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Update the table name to match the one defined in your Terraform configuration
table = dynamodb.Table('table1')

sns = boto3.client('sns')
sns_topic_arn = 'arn:aws:sns:us-east-1:934036565719:notify'  # SNS topic ARN

def compare_files(raw_data, updated_data):
    changed_records = []

    # Get the current timestamp
    timestamp = str(datetime.now())

    # Iterate through each record in the raw data
    for index, (raw_record, updated_record) in enumerate(zip(raw_data, updated_data), 1):
        if raw_record != updated_record:
            changed_records.append({
                'ID': f'record_{index}',  # Generate unique ID for each record
                'OldRecord': raw_record,
                'NewRecord': updated_record,
                'ModificationTime': timestamp  # Add timestamp indicating when the modification occurred
            })

    return changed_records

def send_notification(message):
    sns.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )

def lambda_handler(event, context):
    try:
        # Define S3 bucket and file names
        bucket_name = 'finaltaskbucket2'
        raw_file_name = 'uploaded_file.csv'
        updated_file_name = 'updated_file.csv'

        # Fetch raw file from S3
        raw_file_response = s3.get_object(Bucket=bucket_name, Key=raw_file_name)
        raw_file_content = raw_file_response['Body'].read().decode('utf-8').splitlines()

        # Fetch updated file from S3
        updated_file_response = s3.get_object(Bucket=bucket_name, Key=updated_file_name)
        updated_file_content = updated_file_response['Body'].read().decode('utf-8').splitlines()

        # Compare files
        changed_records = compare_files(raw_file_content, updated_file_content)

        # Store changed records in DynamoDB
        for record in changed_records:
            table.put_item(Item=record)

        # Send notification
        send_notification("Sanity check completed and changes stored in DynamoDB")

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Comparison completed and changes stored in DynamoDB. Notification sent.'})
        }
    except Exception as e:
        # Return error response if any exception occurs
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
