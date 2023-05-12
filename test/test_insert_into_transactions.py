from unittest.mock import MagicMock, call
from src.load_database import insert_into_transactions_table

def test_insert_into_transactions_table():
    connection = MagicMock()

    transactions = [{'date_time': '2011-01-01 09:00',
             'location': 'chesterfield',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             'temp_transaction_id': 62
             }]
             
    items  =[{"item_name": "Large Filter coffee",
         "item_price": '1.80',
         "temp_transaction_id": 62},
        {"item_name": "Regular Filter coffee",
         "item_price": '1.50',
         "temp_transaction_id": 62},
        {"item_name": "Large Iced americano",
         "item_price": '2.50',
         "temp_transaction_id": 62},
        {"item_name": "Large Filter coffee",
         "item_price": '1.80',
         "temp_transaction_id": 1},
        {"item_name": "Large Hot Chocolate",
         "item_price": '1.70',
         "temp_transaction_id": 5},
        {"item_name": "Large Iced americano",
         "item_price": '2.50',
         "temp_transaction_id": 2},
        {"item_name": "Large Iced americano",
         "item_price": '2.50',
         "temp_transaction_id": 2}]
    connection.cursor().fetchone.side_effect = [(1, ),(1, ), None]

    insert_into_transactions_table(connection, transactions, items)

    connection.cursor().execute.assert_has_calls([
        call("SELECT location_id FROM locations WHERE location_name = 'Chesterfield' LIMIT (1)"),
        call("SELECT payment_id FROM payment_types WHERE payment = 'CARD' LIMIT (1)"),
        call("SELECT * FROM transactions WHERE date_time = '2011-01-01 09:00' AND location_id = '1' AND total_price = '3.7' LIMIT (1)"),
        call(''' INSERT INTO transactions(date_time, location_id, total_price, payment_id)
                VALUES (%s, %s, %s, %s)
                RETURNING transaction_id;
                ''', ('2011-01-01 09:00', 1, '3.7', 1))
        
    ])    