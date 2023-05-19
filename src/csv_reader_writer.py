import boto3
import csv

def extract_csv_from_raw_data_bucket(bucket_name, file_name, s3, column_names):
    """
    This function extract CSV file from raw data bucket and puts it into a list of dictionaries.
    """

    transaction_list = []

    try:        
        csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(f"Getting csv file: bucket name = {bucket_name}, key = {file_name}")
        transactions = csv_file['Body'].read().decode('utf-8').splitlines()
        print(f"Read csv file: bucket name = {bucket_name}, key = {file_name}")
        reader = csv.reader(transactions)

        for line in reader:   
            transaction_entry = {} 
            for i, column_name in enumerate(column_names):
                transaction_entry[column_name] = line[i]
               
            transaction_list.append(transaction_entry)
        print(f'Extracted csv file: Rows = {len(transaction_list)}, bucket name = {bucket_name}')        
        return transaction_list
        
    except Exception as e:
        print(f"Lambda Extracting error = {e}")

def extract_csv_from_bucket_with_column_names(bucket_name, file_name, s3):
    """
    This function extract CSV file from transformed data bucket and puts it into a list.
    """   

    transaction_list = []

    try:        
        csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(f"Getting csv file: bucket name = {bucket_name}, key = {file_name}")
        transactions = csv_file['Body'].read().decode('utf-8').splitlines()
        print(f"Read csv file: bucket name = {bucket_name}, key = {file_name}")
        reader = csv.DictReader(transactions)

        for line in reader:   
            transaction_list.append(line)
               
        print(f'Extracted csv file: Rows = {len(transaction_list)}, bucket name = {bucket_name}')        
        return transaction_list
        
    except Exception as e:
        print(f"Lambda Extracting error = {e}")

def write_csv(file, data):
    """
    This function writes a new CSV file.
    """

    with open(file, 'w+') as f:
        dict_writer = csv.DictWriter(f, fieldnames = data[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(data)