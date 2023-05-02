import psycopg2
import os
from dotenv import load_dotenv

""" This module creates a connection with PostgreSQL. 
It has functions to create tables for: items, locations, 
payment_type, transaction-items and transaction. 
"""

# Gets the login info to database
load_dotenv()
host_name = os.environ.get("postgres_host")
user_name = os.environ.get("postgres_user")
user_password = os.environ.get("postgres_pass")
database_name = os.environ.get("postgres_db")

# Sets up the connections 
def setup_db_connection(host=host_name, 
                        user=user_name, 
                        password=user_password,
                        db=database_name):
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

if __name__ == '__main__':
    connection = setup_db_connection()
    create_items_table(connection)
    create_payment_types_table(connection)
    create_locations_table(connection)
    create_transaction_table(connection)
    create_transaction_items_table(connection)
    
    