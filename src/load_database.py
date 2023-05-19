import boto3
import create_database as cdb
import json
from csv_reader_writer import extract_csv_from_bucket_with_column_names

""" This module has functions to insert into Redshift database: 
items table, locations table, transaction-items table and transaction table. 
This module has function to check if entries are duplicate.
"""

def get_unique_items(basket_item_list):
    """
    This function creates and returns unique items list of dictionaries.
    """

    item_list = [] # list of strings, name of item
    unique_items = [] # list of dictionaries, item data
    for dict in basket_item_list:
        if dict['item_name'] not in item_list:
            item_list.append(dict['item_name']) # append item name to list for filtering
            unique_items.append(dict) # append item data to list to return at end
    return unique_items

def get_unique_locations(transaction_list):
    """
    This function creates and returns unique locations list.
    """

    unique_locations = [] # list of strings, name of location
    for dict in transaction_list:
        if dict['location'] not in unique_locations:
            unique_locations.append(dict['location']) # append location name to return at end
    return unique_locations

# takes unique location names and inserts into locations table
def insert_into_location_table(connection, unique_location_list):
    try:
        print('insert_into_location_table started')
        cursor = connection.cursor()

        # depends if the column is a dictionary in a list
        for location_name in unique_location_list:
            
            if check_if_duplicate_entry(connection, 'locations', location_name, 'location_name'):
                continue    # checks if entries already there, if it finds, it ignores the duplicates 
            else:
                insert_location_to_db = ''' INSERT INTO locations(location_name)
                VALUES (%s);
                '''  #it inserts non-duplicate entries

                # we need to make tuple to be able to execute the cursor on postgres!
                cursor.execute(insert_location_to_db, (location_name,)) 
                connection.commit()

        cursor.close()
        print('insert_into_location_table completed')
    except Exception as e:
        print(f'insert_into_location_table error: {e}')

# takes items and inserts into items table
def insert_into_item_table(connection, items_list):
    try:
        print('insert_into_item_table started')
        cursor = connection.cursor()

        # Looping each item in the item list
        for item in items_list:
            sql = "SELECT * FROM items WHERE item_name = '" + item['item_name'] + "' AND item_price = '" + item['item_price'] + "' LIMIT (1);"
            cursor.execute(sql)
            if cursor.fetchone():
                continue  # checks if entries already there, if it finds, it ignores the duplicates 
            else:
                insert_item_to_db = ''' INSERT INTO items(item_name, item_price)
                VALUES (%s, %s);
                ''' #it inserts non-duplicate entries 

                # assuming the key names in a dictionary is item_name  and item_price
                item_values = (item['item_name'], item['item_price'])
                
                cursor.execute(insert_item_to_db, item_values)
                connection.commit()

        cursor.close()
        print('insert_into_item_table completed')
    except Exception as e:
        print(f'insert_into_item_table error: {e}')

# checks the database table if there is a similar entry before inserting
def check_if_duplicate_entry(connection, table: str, entry: str, column_name: str):
    try:
        print('check_if_duplicate_entry started')
        cursor = connection.cursor()

        sql = "SELECT * FROM " + table + " WHERE " + column_name + " = " + f"'{entry}'" + " LIMIT (1)"
        
        cursor.execute(sql)
        print('check_if_duplicate_entry completed')
        if cursor.fetchone():
            return True
        else:
            return False
        
    except Exception as e:
        print(f'check_if_duplicate_entry error: {e}')
    
def insert_into_transactions_table(connection, transactions, items):
    try:
        print('insert_into_transactions_table started')
        cursor = connection.cursor()

        # Looping each entry in the transaction list
        for transaction in transactions:

            location_name = transaction['location']
            sql_location = "SELECT location_id FROM locations WHERE location_name = " + f"'{location_name}'" + " LIMIT (1)"
            cursor.execute(sql_location)
            location_id = cursor.fetchone()[0]

            payment_method = transaction['payment_method']
            sql_payment = "SELECT payment_id FROM payment_types WHERE payment = " + f"'{payment_method}'" + " LIMIT (1)"
            cursor.execute(sql_payment)
            payment_id = cursor.fetchone()[0]

            # has own check if duplicate since multiple values needed to verify match
            sql = "SELECT * FROM transactions WHERE date_time = '" + transaction['date_time'] + "' AND location_id = '" + str(location_id) + "' AND total_price = '" + transaction['total_price'] + "' LIMIT (1)"
            cursor.execute(sql)
            if cursor.fetchone():
                continue
            else:
                insert_transaction_to_db = ''' INSERT INTO transactions(date_time, location_id, total_price, payment_id)
                VALUES (%s, %s, %s, %s);
                '''
                transaction_data = (transaction['date_time'], location_id, transaction['total_price'], payment_id)
                cursor.execute(insert_transaction_to_db, transaction_data)

                get_transaction_id = "SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT 1;"

                cursor.execute(get_transaction_id)
                transaction_id = cursor.fetchone()[0]
                
                insert_into_transaction_items_table(connection, transaction_id, transaction['temp_transaction_id'], items)
            connection.commit()

        cursor.close()
        print('insert_into_transactions_table completed')
    except Exception as e:
        print(f'insert_into_transactions_table error: {e}')

def insert_into_transaction_items_table(connection, transaction_id, temp_transaction, items):
    try:
        # print('insert_into_transaction_items_table started')
        cursor = connection.cursor()

        for item in items:
            if item['temp_transaction_id'] == temp_transaction:
                item_name = item['item_name']
                sql_item = "SELECT item_id FROM items WHERE item_name = " + f"'{item_name}'" + " LIMIT (1)"

                cursor.execute(sql_item)
                item_id = cursor.fetchone()[0]

                insert_transaction_item_to_db = """INSERT INTO transaction_items(transaction_id, item_id)
                VALUES (%s, %s);"""

                transaction_item_data = (transaction_id, item_id)

                cursor.execute(insert_transaction_item_to_db, transaction_item_data)

            connection.commit()

        cursor.close()
        # print('insert_into_transaction_items_table completed')
    except Exception as e:
        print(f'insert_into_transaction_items_table error: {e}')

def lambda_handler(event, context):
    """
    This lambda function gets transformed data from AWS S3 bucket and loads it to AWS Redshift.
    """

    print(f"cool-beans-load-function: invoked, event={event}")
    try:
        s3 = boto3.client('s3')
        for msg_id, msg in enumerate(event['Records']):
            print(f'lambda_handler: message_id = {msg_id}')
            message_body = msg['body']
            message_body_json = json.loads(message_body)
            print('lambda_handler: message_body_json loaded okay')

            transactions_file = message_body_json['transactions_key']
            baskets_file = message_body_json['baskets_key']
            
            bucket_name = message_body_json['bucket']
            print(f'Lambda Handler: bucket name = {bucket_name}, file = {transactions_file}') #CHECKS FOR CORRECT CSV FILE/BUCKET
            print(f'Lambda Handler: bucket name = {bucket_name}, file = {baskets_file}') #CHECKS FOR CORRECT CSV FILE/BUCKET

            transactions = extract_csv_from_bucket_with_column_names(bucket_name, transactions_file, s3)
            baskets = extract_csv_from_bucket_with_column_names(bucket_name, baskets_file, s3)

            unique_items = get_unique_items(baskets)
            unique_locations = get_unique_locations(transactions)

            print('Starting set up connection redshift')
            ssm_client = boto3.client('ssm')
            parameter_details = ssm_client.get_parameter(Name='cool-beans-redshift-settings')
            redshift_details = json.loads(parameter_details['Parameter']['Value'])

            # Gets the login info to database
            rs_host = redshift_details['host']
            rs_port = redshift_details['port']
            rs_database_name = redshift_details['database-name']
            rs_user = redshift_details['user']
            rs_password = redshift_details['password']
            print('Completed retrieving the connection details')

            # GETTING CONNECTION
            connection = cdb.setup_db_connection(host=rs_host, 
                                            user=rs_user, 
                                            password=rs_password,
                                            db=rs_database_name,
                                            port = rs_port)

            #INSERTING INTO DATABASE
            insert_into_location_table(connection, unique_locations)
            insert_into_item_table(connection, unique_items)
            insert_into_transactions_table(connection, transactions, baskets)

    except Exception as e:
        print(f"Lambda Handler Error = {e}") 