# from unittest.mock import patch
# from src.extract_transform import read_csv_to_list

# """
# This module tests read_csv_to_list function in extract_transform.py module    
# """

# @patch('builtins.open')
# @patch('csv.reader')
# def test_read_csv_to_lists(mock_csv_reader, test_file):  
#     mock_csv_reader.return_value = [
#         [
#             '1', 'chesterfield', 'jack', 'coffee - 3.7', '3.7', 'card', '1234567890'
#         ],
#     ]
    
#     expected_transactions = [
#             {'date_time': '1',
#              'location': 'chesterfield',
#              "customer_name": 'jack',
#              "basket": 'coffee - 3.7',
#              'total_price': '3.7',
#              'payment_method': 'card',
#              "card_number": '1234567890'}
#            ]
        
#     test_file = None
      
#     result_transaction = read_csv_to_list(test_file)
    
#     assert expected_transactions == result_transaction