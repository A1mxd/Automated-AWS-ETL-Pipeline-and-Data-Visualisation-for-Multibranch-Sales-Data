import load_database as db
import extract_transform as et
from create_database import setup_db_connection 


"""This module calls the functions from extract_transform module\
and load_database module to load tables into the database.
"""

transactions = et.read_all_csv_files()

sensitive_data = ["customer_name", "card_number"]
et.remove_sensitive_data(transactions, sensitive_data)

baskets = et.create_item_list(transactions)

transactions = et.convert_all_dates(transactions, ['date_time'])

unique_items = et.get_unique_items(baskets)

unique_locations = et.get_unique_locations(transactions)


connection = setup_db_connection()
db.insert_into_location_table(connection, unique_locations)
db.insert_into_item_table(connection, unique_items)
db.insert_into_transactions_table(connection, transactions, baskets)