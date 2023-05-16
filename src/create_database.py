import psycopg2

""" This module creates a connection with PostgreSQL. 
It has functions to create tables for: items, locations, 
payment_type, transaction-items and transaction. 
"""

# Sets up the connections 
def setup_db_connection(host, 
                        user, 
                        password,
                        db,
                        port):
    try:
        print('setup_db_connection started')
        connection = psycopg2.connect(
            host = host,
            dbname = db,
            user = user,
            password = password,
            port = port
        )
        print('setup_db_connection completed') 
        
        return connection
    except Exception as e:
        print(f'setup_db_connection error: {e}')

# Creates the items tables
def create_items_table(connection):
    try:
        print('create_items_table started')
        cursor = connection.cursor()

        create_item_table = """CREATE TABLE IF NOT EXISTS items(
            item_id INT identity(1, 1) PRIMARY KEY,
            item_name VARCHAR(200),
            item_price DECIMAL(19,2));
        """

        cursor.execute(create_item_table)
        connection.commit()
        cursor.close()
        print('create_items_table completed')

    except Exception as e:
        print(f'create_items_table error: {e}')

# Creates the payment type table
def create_payment_types_table(connection):
    try:
        print('create_payment_types_table started')
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS payment_types (\
            payment_id INT identity(1, 1) PRIMARY KEY,\
            payment VARCHAR(5));")
            
        cursor.execute("TRUNCATE TABLE payment_types;")
        cursor.execute("INSERT INTO payment_types(payment) \
                        VALUES ('CARD'), ('CASH');")
        
        connection.commit()
        cursor.close()
        print('create_payment_types_table completed')

    except Exception as e:
        print(f'create_payment_types_table error: {e}')

# Creates locations table
def create_locations_table(connection):
    try:
        print('create_locations_table started')
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS locations (\
            location_id INT identity(1, 1) PRIMARY KEY,\
            location_name VARCHAR(200));")
        
        connection.commit()
        cursor.close()
        print('create_locations_table completed')

    except Exception as e:
        print(f'create_locations_table error: {e}')

# Creates transaction table
def create_transaction_table(connection):
    try:
        print('create_transaction_table started')
        cursor = connection.cursor()

        create_transaction_table = """CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INT identity(1, 1) PRIMARY KEY,
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
        print('create_transaction_table completed')

    except Exception as e:
        print(f'create_transaction_table error: {e}')

# Creates transaction_items table
def create_transaction_items_table(connection):
    try:
        print('create_transaction_items_table started')
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
        print('create_transaction_items_table completed')

    except Exception as e:
        print(f'create_transaction_items_table error: {e}')