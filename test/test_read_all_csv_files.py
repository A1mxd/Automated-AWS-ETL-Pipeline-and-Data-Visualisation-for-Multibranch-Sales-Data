from unittest.mock import patch
from src.extract_transform import read_all_csv_files

"""
This module tests read_all_csv_files function in extract_transform.py module    
"""

@patch('csv.reader')
def test_read_all_csv_files(mock_csv_reader):  
    mock_csv_reader.return_value = [
        [
            '1', 'chesterfield', 'jack', 'coffee - 3.7', '3.7', 'card', '1234567890'
        ]
    ]
    
    expected_transactions = [
            {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'},

             {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'},
             
             {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
              
    result_transaction = read_all_csv_files()
    
    assert expected_transactions == result_transaction