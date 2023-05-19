import boto3
import os
import logging
import extract_transform as et
import csv_reader_writer as cr
import json
    
def lambda_handler(event, context):
    """
    This lambda function gets raw data from AWS S3 bucket, transformes it and 
    loads it to another AWS S3 bucket.
    """

    print(f"cool-beans-extract-transform-function: invoked, event={event}")
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
            column_names = ['date_time', 'location', 'customer_name', 'basket', 'total_price', 'payment_method', 'card_number']
            transactions = cr.extract_csv_from_raw_data_bucket(bucket_name, file_name, s3, column_names)
            
            # TRANSFORMING
            sensitive_data = ["customer_name", "card_number"]
            et.remove_sensitive_data(transactions, sensitive_data)
            
            baskets = et.create_item_list(transactions)

            transactions = et.convert_all_dates(transactions, ['date_time'])

            cr.write_csv("/tmp/baskets.csv", baskets)
            cr.write_csv('/tmp/transactions.csv', transactions)

            # SENDING TO S3
            transformed_bucket_name = 'cool-beans-transformed-data'
            tranformed_baskets_name = file_name[:-4] + '_baskets.csv'

            transformed_transactions_name =file_name[:-4] + '_transactions.csv'


            s3.upload_file("/tmp/baskets.csv", transformed_bucket_name, tranformed_baskets_name)
            print(f"Uploading to S3 into bucket {bucket_name} with key {tranformed_baskets_name}")

            s3.upload_file("/tmp/transactions.csv", transformed_bucket_name, transformed_transactions_name)
            print(f"Uploading to S3 into bucket {bucket_name} with key {transformed_transactions_name}")
            
            # SENDING MESSAGE TO SQS
            message ={
                'bucket': 'cool-beans-transformed-data',
                'transactions_key': "transactions.csv",
                'baskets_key': "baskets.csv"
        }
            json_message = json.dumps(message)

            queue_url = os.environ.get("cool_beans_transform_to_load_queue_url")

            print(f"Sending SQS message {json_message} to queue {queue_url}")
            sqs.send_message(
                 QueueUrl = queue_url,
                 MessageBody = json_message,
                 MessageGroupId = '1') #Always same group ID to limit concurrency

    except Exception as e:
        print(f"Lambda Handler Error = {e}") 

# if __name__ == '__main__':
#     bucket_name = 'cool_beans'
#     file_name = 'london'
#     s3 = 
#     extract_csv_from_bucket(bucket_name, file_name, s3, column_names)
