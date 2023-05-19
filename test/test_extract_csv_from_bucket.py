from unittest.mock import patch, MagicMock, call
from src.csv_reader_writer import extract_csv_from_raw_data_bucket, extract_csv_from_bucket_with_column_names, write_csv

"""
This module tests read_csv_to_list function in extract_transform.py module    
"""

@patch('builtins.open')
@patch('builtins.print')
@patch('csv.reader')
def test_extract_csv_from_raw_data_bucket(mock_csv_reader, mock_print, test_file):  
    mock_bucket_name = 'cool-beans'
    s3 = MagicMock()
    column_names = ['date_time', 'location', 'customer_name', 'basket', 'total_price', 'payment_method', 'card_number']
    
    mock_csv_reader.return_value = [
        [
            '1', 'chesterfield', 'jack', 'coffee - 3.7', '3.7', 'card', '1234567890'
        ],
    ]
    
    expected_transactions = [
            {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
        
    test_file = None
      
    result_transaction = extract_csv_from_raw_data_bucket(mock_bucket_name, test_file, s3, column_names)
    
    assert expected_transactions == result_transaction
    assert mock_print.call_args_list == [call('Getting csv file: bucket name = cool-beans, key = None'), 
                                         call('Read csv file: bucket name = cool-beans, key = None'),
                                         call('Extracted csv file: Rows = 1, bucket name = cool-beans')]

@patch('builtins.open')
@patch('builtins.print')
@patch('csv.DictReader')
def test_extract_csv_from_bucket_with_column_names(mock_csv_reader, mock_print, test_file):  
    mock_bucket_name = 'cool-beans'
    s3 = MagicMock()
    
    mock_csv_reader.return_value = [
            {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
    
    expected_transactions = [
            {'date_time': '1',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
        
    test_file = None
      
    result_transaction = extract_csv_from_bucket_with_column_names(mock_bucket_name, test_file, s3)
    
    assert expected_transactions == result_transaction
    assert mock_print.call_args_list == [call('Getting csv file: bucket name = cool-beans, key = None'), 
                                         call('Read csv file: bucket name = cool-beans, key = None'),
                                         call('Extracted csv file: Rows = 1, bucket name = cool-beans')]

# @patch('builtins.open')
# @patch('builtins.print')
# @patch('csv.DictWriter')
# @patch('csv.DictWriter.writeheader')
# def test_extract_csv_from_bucket_with_column_names(mock_csv_writer, mock_header, mock_print, test_file):
#     test_file = None

#     data = [{'location': 'london'}]

#     mock_csv_writer.side_effect = data
#     mock_header.side_effect = data[0]

#     expected = [{'location': 'london'}]
#     result = write_csv(test_file, data)

#     assert expected == result
    