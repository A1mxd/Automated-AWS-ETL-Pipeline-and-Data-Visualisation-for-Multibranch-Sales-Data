from src.extract_transform import convert_all_dates

"""
This module tests test_read_csv_to_lists function in extract_transform.py module    
"""


def test_convert_all_dates():  
    transactions = [
            {'date_time': '01/01/2011 09:00',
             'location': 'chesterfield',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card'}
           ]
    
    expected = [
            {'date_time': '2011-01-01 09:00',
             'location': 'chesterfield',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card'}
           ]
    
    result_transaction = convert_all_dates(transactions, ['date_time'])
    
    assert expected == result_transaction