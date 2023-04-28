from extract_transform import remove_sensitive_data

"""
This module tests remove_sensitive_data function in extract_transform.py module    
"""

def test_remove_sensitive_data_multiple_keys():  
    transactions = [
            {'date_time': '01/01/2011 09:00',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
    
    expected = [
            {'date_time': '01/01/2011 09:00',
             'location': 'chesterfield',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card'}
           ]
        
    sensitive_data = ["customer_name", "card_number"]
      
    remove_sensitive_data(transactions, sensitive_data)
    
    assert expected == transactions


def test_remove_sensitive_data_single_key():
    transactions = [
            {'date_time': '01/01/2011 09:00',
             'location': 'chesterfield',
             "customer_name": 'jack',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             "card_number": '1234567890'}
           ]
    
    expected = [
            {'date_time': '01/01/2011 09:00',
            'location': 'chesterfield',
            'customer_name': 'jack',
            'basket': 'coffee - 3.7',
            'total_price': '3.7',
            'payment_method': 'card'}
            ]
    
    sensitive_data = ["card_number"]
      
    remove_sensitive_data(transactions, sensitive_data)
    
    assert expected == transactions


def test_remove_sensitive_data_no_keys():
    transactions = [
            {'date_time': '01/01/2011 09:00',
            'location': 'chesterfield',
            "customer_name": 'jack',
            "basket": 'coffee - 3.7',
            'total_price': '3.7',
            'payment_method': 'card',
            "card_number": '1234567890'}
        ]
        
    expected = [
            {'date_time': '01/01/2011 09:00',
            'location': 'chesterfield',
            "customer_name": 'jack',
            "basket": 'coffee - 3.7',
            'total_price': '3.7',
            'payment_method': 'card',
            "card_number": '1234567890'}
        ]
    
    sensitive_data = []
      
    remove_sensitive_data(transactions, sensitive_data)
    
    assert expected == transactions



def test_remove_sensitive_data_nonexistent_keys():
    transactions = [
            {'date_time': '01/01/2011 09:00',
            'location': 'chesterfield',
            "basket": 'coffee - 3.7',
            'total_price': '3.7',
            'payment_method': 'card'}
    ]

    expected = [
            {'date_time': '01/01/2011 09:00',
            'location': 'chesterfield', 
            "basket": 'coffee - 3.7',
            'total_price': '3.7',
            'payment_method': 'card'}
    ]
    
    sensitive_data = ["customer_name", "card_number"]
    
    remove_sensitive_data(transactions, sensitive_data)
    
    assert expected == transactions