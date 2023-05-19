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
    """
    This function takes list of dictionaries and key of date column, 
    and converts date_time value."
    """
    
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
 
    transaction_entry = [{
                    "date_time": '18/05/2023 10:05',
                    "location" : 'London'
                    }]
    date = convert_all_dates(transaction_entry, ['date_time'])
    
    print(date[0]['date_time'])