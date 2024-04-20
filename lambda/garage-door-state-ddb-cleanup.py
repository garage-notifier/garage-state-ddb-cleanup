import os
from datetime import datetime, timedelta

import boto3
import pytz
from botocore.exceptions import ClientError

# Initialize DynamoDB outside of the handler to take advantage of Lambda's execution context reuse
dynamodb = boto3.resource('dynamodb')


def delete_old_entries(table_name, days_to_keep, dynamodb):
    # Convert days_to_keep to an integer and validate
    try:
        days_to_keep = int(days_to_keep)
    except ValueError:
        print("The 'days_to_keep' parameter must be an integer.")
        return {
            'statusCode': 400,
            'body': "The 'days_to_keep' parameter must be an integer."
        }

    # Get the current time in US Central Time Zone
    central_tz = pytz.timezone('US/Central')
    cutoff_date = datetime.now(central_tz) - timedelta(days=days_to_keep)

    table = dynamodb.Table(table_name)

    # Scan the table for items older than the cutoff date
    try:
        response = table.scan(
            FilterExpression="garage_door_status_timestamp < :cutoff_date",
            ExpressionAttributeValues={
                ':cutoff_date': cutoff_date.isoformat()
            }
        )
        items = response.get('Items', [])
        for item in items:
            # Delete items found in the scan
            table.delete_item(
                Key={
                    'id': item['id'],
                    'garage_door_status_timestamp': item['garage_door_status_timestamp']
                }
            )
        return {
            'statusCode': 200,
            'body': f"Total {len(items)} items deleted."
        }
    except ClientError as e:
        print(f"Failed to delete old entries: {e.response['Error']['Message']}")
        return {
            'statusCode': 500,
            'body': f"Failed to delete old entries: {e.response['Error']['Message']}"
        }


def lambda_handler(event, context):
    # Fetch configuration from environment variables
    table_name = os.getenv('DYNAMODB_TABLE', 'default-table-name')
    days_to_keep = os.getenv('DAYS_TO_KEEP', '3')
    # Call the delete_old_entries function with the DynamoDB resource
    result = delete_old_entries(table_name, days_to_keep, dynamodb)
    return result
