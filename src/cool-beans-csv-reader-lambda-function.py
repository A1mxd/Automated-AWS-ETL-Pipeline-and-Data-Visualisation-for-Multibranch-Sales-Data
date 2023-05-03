import boto3
import csv
import os
import logging

s3 = boto3.client('s3')
def extract_csv_from_bucket(bucket_name, file_name):
    
    transaction_list = []

    try:
        
        csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(f"Getting csv file: bucket name ={bucket_name}, key={file_name}")
        transactions = csv_file['Body'].read().decode('utf-8').splitlines()
        print(f"Read csv file: bucket name ={bucket_name}, key={file_name}")
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
        print(f'Extracted csv file: Rows = {len(transaction_list)}, bucket name = {bucket_name} ')        
        return transaction_list
        
    except Exception as e:
        print(f"Lambda Extracting error = {e}")
    
def lambda_handler(event, context):
    # TODO implement
    try:
        # file name inputted in the s3 bucket
        file_name = event['Records'][0]['s3']['object']['key']
        # folder_path = '2023/4/26/' #testing folder path
        # file = folder_path + file_name
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        print(f'Lambda Handler: bucket name = {bucket_name}, file = {file_name}')
        # print(f"delon9-cool-bean-csv-reader: invoked, event={event}")
        list = extract_csv_from_bucket(bucket_name, file_name)
        return {
            'statusCode': 200,
            'body': list
        }
    except Exception as e:
        print(f"Lambda Handler Error = {e}") 

    