import csv

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



if __name__ == '__main__':

    transactions, baskets = read_csv_to_lists("data/chesterfield_25-08-2021_09-00-00.csv")

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
