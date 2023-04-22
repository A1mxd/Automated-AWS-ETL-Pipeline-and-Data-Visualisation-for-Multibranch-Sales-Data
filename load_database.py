from create_database import setup_db_connection
import re

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
            # items = ["locations", "location_name"]

            # a1 = 'samp"le s"tring'
            # a2 = re.sub('"','',a1)
            # print(a2)   

            # items = [re.sub(r'""', {locations}, item) for item in items]
            # items = [re.sub(r'^column_name\b', '{column_name}', item) for item in items]

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

    except Exception as e:
        print(f'Failed to open connection: {e}')

# takes items and inserts into items table
def insert_into_item_table(connection, items_list):
    try:
        cursor = connection.cursor()

        # Looping each item in the item list
        for item in items_list:   
            item_name = item['item_name'] 
            if check_if_duplicate_entry(connection, 'items', item_name, 'item_name'):
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
def check_if_duplicate_entry(connection, table: str, entry: str, column_name: str):
    try:
        cursor = connection.cursor()

        sql = "SELECT * FROM " + table + " WHERE " + column_name + " = " + f"'{entry}'" + " LIMIT (1)"
        
        cursor.execute(sql)

        if cursor.fetchone():
            return True
        else:
            return False
        
    except Exception as e:
        print(f'Failed to open connection: {e}')
    


def insert_into_transaction_items_table(connection, transactions, items):
    pass

