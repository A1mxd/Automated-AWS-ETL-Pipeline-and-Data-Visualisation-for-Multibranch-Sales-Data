import json
import boto3
import csv
import os
import datetime

s3 = boto3.client('s3')
def extract_csv_from_bucket(bucket_name, file):
    
    transaction_list = []

    try:
        
        csv_file = s3.get_object(Bucket=bucket_name, Key=file)
        c = csv_file['Body'].read().decode('utf-8').splitlines()
        reader = csv.reader(c)

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
                
                
        return transaction_list
    
    except:
        print(f"failed to open {file}")

def lambda_handler(event, context):
    # TODO implement
    
    file_name = event['Records'][0]['s3']['object']['key']
    folder_path = '2023/4/26/'
    file = folder_path + file_name
    
    # time of the event triggered - to be able to get the folder located
    time_event_str = event['Records'][0]["eventTime"]
    time_event = datetime.datetime.strptime(time_event_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    year = time_event.year
    year_str = str(year)
    month = time_event.month
    month_str = str(int(month))
    day = time_event.day
    day_str = str(int(day))
    folder_path_triggered = f'{year_str}/{month_str}/{day_str}/'
    # file = folder_path_triggered + file_name
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    # print(f"delon9-cool-bean-csv-reader: invoked, event={event}")
    list = extract_csv_from_bucket(bucket_name, file)
    return {
        'statusCode': 200,
        'body': list
    }