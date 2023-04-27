import os
import csv
from datetime import datetime

"""This module reads all CSV files and extracts the data from them.
It removes sensitive data and get unique items and locations names.
"""

def read_all_csv_files():
    """ This functions takes all csv files and reads them one by one.
    """
    all_transactions = []

    csv_dir = './data'

    folder = os.listdir(csv_dir) #makes list of files

    # for chunk in chunked_file_list:
    for file in folder:
        file = f'data/{file}' # adds 'data/' path in front of the file names
        transactions = read_csv_to_list(file) # calls read_csv_to_lists() function to read all files one by one
        all_transactions += transactions

    return all_transactions


def read_csv_to_list(file):

    transaction_list = []

    try:
        with open(file, "r") as f:
            reader = csv.reader(f)

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
                
        return transaction_list
    
    except:
        print(f"failed to open {file}")


def remove_sensitive_data(transactions, sensitive_data):
    for transaction in transactions:
        for data in sensitive_data:
            del transaction[data]


def create_item_list(transactions):
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
    # Uniformity
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


if __name__ == '__main__':

    #transactions = read_csv_to_list("data/chesterfield_25-08-2021_09-00-00.csv")
    transactions = read_all_csv_files()

    sensitive_data = ["customer_name", "card_number"]
    remove_sensitive_data(transactions, sensitive_data)

    items = create_item_list(transactions)

    transactions = convert_all_dates(transactions, ['date_time'])

    print(len(transactions), len(items))

    for i, transaction in enumerate(transactions):
        if i < 5:
            print(transaction)

    for i, item in enumerate(items):
        if i < 5:
            print(item)

    unique_items = get_unique_items(items)
    for entry in unique_items:
        print(entry)

    unique_locations = get_unique_locations(transactions)
    for entry in unique_locations:
        print(entry)
