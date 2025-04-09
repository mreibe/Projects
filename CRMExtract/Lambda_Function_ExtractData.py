import json
import requests
import boto3
from datetime import datetime

# Initialize SQS client
sqs = boto3.client('sqs')

CRM_API_URL = 'https://crm.example.com/api/v1/records'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123456789012/CRMDataQueue'

def lambda_handler(event, context):
    # Fetch data from CRM API
    response = requests.get(CRM_API_URL)
    if response.status_code == 200:
        crm_data = response.json()
        
        # Send data to SQS
        for record in crm_data['data']:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(record)
            )
        return {
            'statusCode': 200,
            'body': json.dumps('Data processed successfully')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to fetch CRM data')
        }