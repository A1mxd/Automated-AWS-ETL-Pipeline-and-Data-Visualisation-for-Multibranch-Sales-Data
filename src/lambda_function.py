import boto3
import csv
import os
import logging
import create_database as cdb
import extract_transform as et
import load_database as db
import json


def extract_csv_from_bucket(bucket_name, file_name, s3):
    
    transaction_list = []

    try:        
        csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(f"Getting csv file: bucket name = {bucket_name}, key = {file_name}")
        transactions = csv_file['Body'].read().decode('utf-8').splitlines()
        print(f"Read csv file: bucket name = {bucket_name}, key = {file_name}")
        reader = csv.reader(transactions)

        for line in reader:
                transaction_entry = {
                    "date_time": line[0],
                    "location" : line[1],
                    "customer_name": line[2],
                    "basket": line[3],
                    "total_price" : line[4],
                    "payment_method" : line[5],
                    "card_number": line[6]
                    }
                transaction_list.append(transaction_entry)
        print(f'Extracted csv file: Rows = {len(transaction_list)}, bucket name = {bucket_name}')        
        return transaction_list
        
    except Exception as e:
        print(f"Lambda Extracting error = {e}")
        
    
def lambda_handler(event, context):
    print(f"cool-bean-etl-function: invoked, event={event}")
    try:
        s3 = boto3.client('s3')
        sqs = boto3.client('sqs')

        for msg_id, msg in enumerate(event['Records']):
            print(f'lambda_handler: message_id = {msg_id}')
            message_body = msg['body']
            message_body_json = json.loads(message_body)
            print('lambda_handler: message_body_json loaded okay')
            file_name = message_body_json['Records'][0]['s3']['object']['key']
            
            bucket_name = message_body_json['Records'][0]['s3']['bucket']['name']
            print(f'Lambda Handler: bucket name = {bucket_name}, file = {file_name}') #CHECKS FOR CORRECT CSV FILE/BUCKET
                  
            #EXTRACTING 
            transactions = extract_csv_from_bucket(bucket_name, file_name, s3)
            
            # TRANSFORMING
            sensitive_data = ["customer_name", "card_number"]
            et.remove_sensitive_data(transactions, sensitive_data)
            
            baskets = et.create_item_list(transactions)

            transactions = et.convert_all_dates(transactions, ['date_time'])

            unique_items = et.get_unique_items(baskets)
            unique_locations = et.get_unique_locations(transactions)

            # SENDING TO S3
            s3.upload_file(bucket_name, file_name)
            print(f"Uploading to S3 into bucket {bucket_name} with key {file_name}")
            
            # SENDING MESSAGE TO SQS
            message ={
            'body_items': unique_items,
            'body_locations': unique_locations,
            'body_transactions': transactions,
            'body_baskets': baskets
        }
            json_message = json.dumps(message)

            queue_url = os.environ.get("https://sqs.eu-west-1.amazonaws.com/015206308301/cool_beans_transform_to_load_queue")

            print(f"Sending SQS message {json_message} to queue {queue_url}")
            sqs.send_message(
                 QueueUrl = queue_url,
                 MessageBody = json_message,
                 MessageGroupId = '1') #Always same group ID to limit concurrency

    except Exception as e:
        print(f"Lambda Handler Error = {e}") 

    
    
    

    
    
