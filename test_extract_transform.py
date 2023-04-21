from unittest.mock import patch
from extract_transform import read_csv_to_lists

"""
This module tests test_read_csv_to_lists function in extract_transform.py module    
"""

@patch('builtins.open')
@patch('csv.reader')
def test_read_csv_to_lists(mock_csv_reader, test_file):  
    mock_csv_reader.return_value = [
        [
            '1', 'chesterfield', 'jack', 'coffee - 3.7', '3.7', 'card', '1234567890'
        ],
        [
            '2', 'leeds', 'sue', 'tea - 3.7', '3.7', 'cash', None
        ],
    ]
    
    expected_transactions = [
            {'temp_transaction_id': 0,
             'date_time': '1',
             'location': 'chesterfield',
             'payment_method': 'card',
             'total_price': '3.7'},

            {'temp_transaction_id': 1,
             'date_time': '2',
             'location': 'leeds',
             'payment_method': 'cash',
             'total_price': '3.7'},
           ]
    
    expected_items = [
            {'temp_basket_item_id': 0,
             'item_name': 'coffee',
             'item_price': '3.7',
             'temp_transaction_id': 0},
             
            {'temp_basket_item_id': 1,
             'item_name': 'tea',
             'item_price': '3.7',
             'temp_transaction_id': 1}
            ]
    

    test_file = None
      
    result_transaction, results_items = read_csv_to_lists(test_file)
    
    assert expected_transactions == result_transaction
    assert expected_items == results_items