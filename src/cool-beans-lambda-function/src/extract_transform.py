import os
import csv
from datetime import datetime

def remove_sensitive_data(transactions, sensitive_data):
    """ This functions takes transaction list and 
    sensitive data(customer_name and card number columns),
    and removes those columns from the each dictionary of transactions.
    """
    for transaction in transactions:
        for data in sensitive_data:
            if data in transaction:
                del transaction[data]
    print(f'Removed sensitive data: Rows = {len(transactions)}') 

def create_item_list(transactions):
    """ This functions takes cleaned transactions. 
    It splits the multiple basket items, 
    separates 'item_name' and 'item_price' from each item,
    and creates a products list for each transaction.
    """

    basket_item_list = []
    temp_transaction_id = 0

    for transaction in transactions:
        transaction["temp_transaction_id"] = temp_transaction_id

        items = transaction["basket"].split(",")

        for item in items:
            item = item.strip().rsplit(" - ", 1)
            basket_item_entry = {
                "item_name": item[0],
                "item_price": item[1],
                "temp_transaction_id": temp_transaction_id
            }
            basket_item_list.append(basket_item_entry)
        temp_transaction_id += 1

    return basket_item_list

def convert_all_dates(list_of_dicts, date_cols, 
                      current_format='%d/%m/%Y %H:%M',
                      expected_format='%Y-%m-%d %H:%M'):

    for dict in list_of_dicts:
        for col in date_cols:
            try:
                str_to_date = datetime.strptime(dict[col], current_format)
                date_to_str = datetime.strftime(str_to_date, expected_format)
                dict[col] = date_to_str
            except ValueError as e:
                print(f"Error parsing value '{dict[col]}' in column '{col}': {e}")
                dict[col] = None
            
    return list_of_dicts


def get_unique_items(basket_item_list):
    item_list = [] # list of strings, name of item
    unique_items = [] # list of dictionaries, item data
    for dict in basket_item_list:
        if dict['item_name'] not in item_list:
            item_list.append(dict['item_name']) # append item name to list for filtering
            unique_items.append(dict) # append item data to list to return at end
    return unique_items

def get_unique_locations(transaction_list):
    unique_locations = [] # list of strings, name of location
    for dict in transaction_list:
        if dict['location'] not in unique_locations:
            unique_locations.append(dict['location']) # append location name to return at end
    return unique_locations
