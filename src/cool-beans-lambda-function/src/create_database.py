import psycopg2
import boto3
import os
import json

""" This module creates a connection with PostgreSQL. 
It has functions to create tables for: items, locations, 
payment_type, transaction-items and transaction. 
"""
ssm_client = boto3.client('ssm')
parameter_details = ssm_client.get_parameter(Name='cool-beans-redshift-settings')
redshift_details = json.loads(parameter_details['Parameter']['Value'])


print('Starting set up connection redshift')
ssm_client = boto3.client('ssm')
parameter_details = ssm_client.get_parameter(Name='cool-beans-redshift-settings')
redshift_details = json.loads(parameter_details['Parameter']['Value'])

# Gets the login info to database
rs_host = redshift_details['host']
rs_port = redshift_details['port']
rs_database_name = redshift_details['database']
rs_user = redshift_details['user']
rs_password = redshift_details['password']
print('Completed retrieving the connection details')

# Sets up the connections 
def setup_db_connection(host=rs_host, 
                        user=rs_user, 
                        password=rs_password,
                        db=rs_database_name):
    connection = psycopg2.connect(
        host = host,
        database = db,
        user = user,
        password = password
    )

    return connection

# Creates the items tables
def create_items_table(connection):
    try:
        cursor = connection.cursor()

        create_item_table = """CREATE TABLE IF NOT EXISTS items(
            item_id SERIAL PRIMARY KEY,
            item_name VARCHAR(200),
            item_price DECIMAL(19,2));
        """

        cursor.execute(create_item_table)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# Creates the payment type table
def create_payment_types_table(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS payment_types (\
            payment_id SERIAL PRIMARY KEY,\
            payment VARCHAR(5));")
        
        cursor.execute("INSERT INTO payment_types \
                        VALUES (1, 'CARD'), (2, 'CASH') ON CONFLICT DO NOTHING;")
        
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# Creates locations table
def create_locations_table(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS locations (\
            location_id SERIAL PRIMARY KEY,\
            location_name VARCHAR(200));")
        
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# Creates transaction table
def create_transaction_table(connection):
    try:
        cursor = connection.cursor()

        create_transaction_table = """CREATE TABLE IF NOT EXISTS transactions (
            transaction_id SERIAL PRIMARY KEY,
            date_time TIMESTAMP, 
            location_id INT,
            total_price DECIMAL(19,2), 
            payment_id INT, 
            FOREIGN KEY (location_id) REFERENCES locations(location_id),
            FOREIGN KEY (payment_id) REFERENCES payment_types(payment_id));
            """
        
        cursor.execute(create_transaction_table)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# Creates transaction_items table
def create_transaction_items_table(connection):
    try:
        cursor = connection.cursor()
        
        create_transaction_item_table = """CREATE TABLE IF NOT EXISTS transaction_items (
            transaction_id INT,
            item_id INT,
            FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
            FOREIGN KEY (item_id) REFERENCES items(item_id));
            """
        
        cursor.execute(create_transaction_item_table)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')