import os
import csv

"""This module reads all CSV files and extracts the data from them.
It removes sensitive data and get unique items and locations names.
"""

# i copied 'relative path' of data folder
# but check if it is working for everyone. 
# if not make sure right click on data folder and 'copy path' and paste it instead
csv_dir = 'cool-beans-final-project\data'

def chunks(list_of_items, n):
    """ This function takes n amount of items and puts them in a list
    """

    n = max(1, n)
    return [list_of_items[i:i+n] for i in range(0, len(list_of_items), n)]

folder = os.listdir(csv_dir) #makes list of files

chunked_file_list = chunks(folder, 100) # makes list of files list up until 100 files

def read_all_csv_files():
    """ This functions takes all csv files and reads them one by one.
    """
    
    for chunk in chunked_file_list:
        for file in chunk:
            file = f'data/{file}' # adds 'data/' path in front of the file names
            read_all_files = read_csv_to_lists(file) # calls read_csv_to_lists() function to read all files one by one

    return read_all_files

def read_csv_to_lists(file):

    transaction_list = []
    basket_item_list = []

    try:
        with open(file, "r") as f:
            reader = csv.reader(f)

            # tempory ids for transctions and items, will be replaced when loaded into database
            temp_transaction_id = 0
            temp_basket_item_id = 0

            for line in reader:

                transaction_entry = {
                    "temp_transaction_id": temp_transaction_id,
                    "date_time": line[0],
                    "location" : line[1],
                    "total_price" : line[4],
                    "payment_method" : line[5]
                    }
                transaction_list.append(transaction_entry)
                
                items = line[3].split(",")
                for item in items:
                    item = item.strip().rsplit(" - ", 1)
                    basket_item_entry = {
                        "temp_basket_item_id": temp_basket_item_id,
                        "item_name": item[0],
                        "item_price": item[1],
                        "temp_transaction_id": temp_transaction_id
                    }
                    basket_item_list.append(basket_item_entry)
                    temp_basket_item_id += 1
                temp_transaction_id += 1

        return transaction_list, basket_item_list
    
    except:
        print(f"failed to open {file}")


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

if __name__ == '__main__':

    transactions, baskets = read_csv_to_lists("data/chesterfield_25-08-2021_09-00-00.csv")
    transactions = convert_all_dates(transactions, ['date_time'])

    for i, transaction in enumerate(transactions):
        if i < 5:
            print(transaction)

    for i, item in enumerate(baskets):
        if i < 5:
            print(item)

    unique_items = get_unique_items(baskets)
    for entry in unique_items:
        print(entry)

    unique_locations = get_unique_locations(transactions)
    for entry in unique_locations:
        print(entry)
