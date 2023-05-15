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
        s3 = boto3.client('s3')
        # file name inputted in the s3 bucket
        file_name = event['Records'][0]['s3']['object']['key']
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        print(f'Lambda Handler: bucket name = {bucket_name}, file = {file_name}') #CHECKS FOR CORRECT CSV FILE/BUCKET
        # print(f"delon9-cool-bean-csv-reader: invoked, event={event}")
        print('Starting set up connection redshift')
        ssm_client = boto3.client('ssm')
        parameter_details = ssm_client.get_parameter(Name='cool-beans-redshift-settings')
        redshift_details = json.loads(parameter_details['Parameter']['Value'])

        # Gets the login info to database
        print(redshift_details)
        rs_host = redshift_details['host']
        rs_port = redshift_details['port']
        rs_database_name = redshift_details['database-name']
        rs_user = redshift_details['user']
        rs_password = redshift_details['password']
        print('Completed retrieving the connection details')

        # CREATING DATABASE
        connection = cdb.setup_db_connection(host=rs_host, 
                                        user=rs_user, 
                                        password=rs_password,
                                        db=rs_database_name,
                                        port = rs_port)
        # cdb.create_items_table(connection)
        # cdb.create_payment_types_table(connection)
        # cdb.create_locations_table(connection)
        # cdb.create_transaction_table(connection)
        # cdb.create_transaction_items_table(connection)
        
        #EXTRACTING 
        transactions = extract_csv_from_bucket(bucket_name, file_name, s3)
        
        # TRANSFORMING
        sensitive_data = ["customer_name", "card_number"]
        et.remove_sensitive_data(transactions, sensitive_data)
        
        baskets = et.create_item_list(transactions)

        transactions = et.convert_all_dates(transactions, ['date_time'])

        unique_items = et.get_unique_items(baskets)
        unique_locations = et.get_unique_locations(transactions)

        #LOADING DATABASE
        db.insert_into_location_table(connection, unique_locations)
        db.insert_into_item_table(connection, unique_items)
        db.insert_into_transactions_table(connection, transactions, baskets)
        
        return {
            'statusCode': 200,
            'body': unique_items
        }
    except Exception as e:
        print(f"Lambda Handler Error = {e}") 

    
    
    

    
    
