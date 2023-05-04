import boto3
import csv
import os
import logging
# import src.create_database as cdb
import src.extract_transform as et
# import src.load_database as db

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
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        print(f'Lambda Handler: bucket name = {bucket_name}, file = {file_name}') #CHECKS FOR CORRECT CSV FILE/BUCKET
        # print(f"delon9-cool-bean-csv-reader: invoked, event={event}")
        
        # CREATING DATABASE
        # connection = cdb.setup_db_connection()
        # cdb.create_items_table(connection)
        # cdb.create_payment_types_table(connection)
        # cdb.create_locations_table(connection)
        # cdb.create_transaction_table(connection)
        # cdb.create_transaction_items_table(connection)
        
        #EXTRACTING 
        transactions = extract_csv_from_bucket(bucket_name, file_name)
        
        # TRANSFORMING
        sensitive_data = ["customer_name", "card_number"]
        et.remove_sensitive_data(transactions, sensitive_data)
        
        baskets = et.create_item_list(transactions)

        transactions = et.convert_all_dates(transactions, ['date_time'])

        unique_items = et.get_unique_items(baskets)
        
        #LOADING DATABASE
        # unique_locations = et.get_unique_locations(transactions)
        # db.insert_into_location_table(connection, unique_locations)
        # db.insert_into_item_table(connection, unique_items)
        # db.insert_into_transactions_table(connection, transactions, baskets)
        return {
            'statusCode': 200,
            'body': baskets
        }
    except Exception as e:
        print(f"Lambda Handler Error = {e}") 

    
    
    
