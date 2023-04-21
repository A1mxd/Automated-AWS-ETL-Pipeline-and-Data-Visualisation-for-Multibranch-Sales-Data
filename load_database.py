from create_database import setup_db_connection

""" This module has functions to insert into: 
items table, locations table, transaction-items table and transaction table. 
This module has function to check if entries are duplicate.
"""

# takes unique location names and inserts into locations table
def insert_into_location_table(connection, unique_location_list):
    try:
        cursor = connection.cursor()

        # depends if the column is a dictionary in a list
        for location_name in unique_location_list:
            if check_if_duplicate_entry(connection, 'location', location_name, 'location_name'):
                continue    # checks if entries already there, if it finds, it ignores the duplicates 
            else:
                insert_location_to_db = ''' INSERT INTO locations(location_name)
                VALUES (%s);
                '''  #it inserts non-duplicate entries

                location_values = (location_name)
                cursor.execute(insert_location_to_db, location_values)
                connection.commit()

        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# takes items and inserts into items table
def insert_into_item_table(connection, items_list):
    try:
        cursor = connection.cursor()

        # Looping each item in the item list
        for item in items_list:    
            if check_if_duplicate_entry(connection, 'items', item, 'item_name'):
                continue    # checks if entries already there, if it finds, it ignores the duplicates 
            else:
                insert_item_to_db = ''' INSERT INTO items(item_name, item_price)
                VALUES (%s, %s);
                ''' #it inserts non-duplicate entries 

                # assuming the key names in a dictionary is item_name  and item_price
                item_values = (item['item_name'], item['item_price'])
                cursor.execute(insert_item_to_db, item_values)
                connection.commit()

        cursor.close()

    except Exception as e:
        print(f'Failed to open connection: {e}')

# checks the database table if there is a similar entry before inserting
def check_if_duplicate_entry(connection, table, entry, column_name):
    try:
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {table} \
                       WHERE {column_name} = {entry}\
                       LIMIT (1)')
        
        if cursor.fetchone():
            return True
        else:
            return False
        
    except Exception as e:
        print(f'Failed to open connection: {e}')
    


def insert_into_transaction_items_table(connection,):
    pass

