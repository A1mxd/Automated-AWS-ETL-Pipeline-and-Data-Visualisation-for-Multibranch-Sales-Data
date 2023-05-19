from unittest.mock import patch
from src.load_database import get_unique_items, get_unique_locations

"""
This module tests unique item/location functions in load_database.py module    
"""

# Testing the items are unique
def test_get_unique_items():
    mock_basket_item_list = [
          {'item_name': 'coffee',
            'item_price': '3.7',
            'temp_transaction_id': 0},
            
            {'item_name': 'coffee',
            'item_price': '3.7',
            'temp_transaction_id': 0}
     ]
    expected_unique_item = [
        {'item_name': 'coffee',
        'item_price': '3.7',
        'temp_transaction_id': 0}
    ]

    result_unique_item = get_unique_items(mock_basket_item_list)

    assert expected_unique_item == result_unique_item

def test_get_unique_location():
    mock_transaction_list = [
    {    "date_time": '25/08/2021 10:54',
        "location" : 'Chesterfield',
        "customer_name": 'Anthony Canal',
        "basket": 'Large Filter coffee - 1.80',
        "total_price" : '1.80',
        "payment_method" : 'CARD',
        "card_number": '8891916618682620'},

        {"date_time": '25/08/2021 10:54',
        "location" : 'Chesterfield',
        "customer_name": 'Anthony Canal',
        "basket": 'Large Filter coffee - 1.80',
        "total_price" : '1.80',
        "payment_method" : 'CARD',
        "card_number": '8891916618682620'},

        {"date_time": '25/08/2021 10:54',
        "location" : 'Leeds',
        "customer_name": 'Anthony Canal',
        "basket": 'Large Filter coffee - 1.80',
        "total_price" : '1.80',
        "payment_method" : 'CARD',
        "card_number": '8891916618682620'}
        
    ]
    expected_unique_location = ['Chesterfield', 'Leeds']

    result_unique_location = get_unique_locations(mock_transaction_list)

    assert expected_unique_location == result_unique_location