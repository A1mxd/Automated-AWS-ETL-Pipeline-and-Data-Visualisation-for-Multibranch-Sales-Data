from unittest.mock import patch
from extract import open_csv_as_dict_list

"""
This module tests open_csv_as_dict_list function in extract.py module    
"""

@patch('builtins.open')
@patch('csv.reader')
def test_open_csv_as_dict_list(mock_csv_reader, test_file):  
    mock_csv_reader.return_value = [
        [
            '1', 'jack', 'card', '3.7', 1, 1, 1
        ],
        [
            '2', 'sue', 'card', '3.7', 1, 1, 1
        ],
    ]
    
    expected = [
            {'card_number': 1,
             'customer_name': 'card',
             'date_time': '1',
             'location': 'jack',
             'order': '3.7',
             'payment_method': 1,
             'total_price': 1},
            {'card_number': 1,
             'customer_name': 'card',
             'date_time': '2',
             'location': 'sue',
             'order': '3.7',
             'payment_method': 1,
             'total_price': 1},
           ]

    test_file = None
      
    result = open_csv_as_dict_list(test_file)
    
    assert expected == result