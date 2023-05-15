from unittest.mock import MagicMock, call
from src.load_database import insert_into_transaction_items_table

def test_insert_into_transaction_items_table():
    
    connection = MagicMock()
    transaction_id = 83

    transaction={'date_time': '2011-01-01 09:00',
             'location': 'chesterfield',
             "basket": 'coffee - 3.7',
             'total_price': '3.7',
             'payment_method': 'card',
             'temp_transaction_id': 62
             }
    
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
    connection.cursor().fetchone.side_effect = [(27, ),(52, ),(28, )]
    
    insert_into_transaction_items_table(connection,transaction_id,transaction['temp_transaction_id'], items)

    connection.cursor().execute.assert_has_calls([
        call("SELECT item_id FROM items WHERE item_name = 'Large Filter coffee' LIMIT (1)"),
        call("""INSERT INTO transaction_items(transaction_id, item_id)
                VALUES (%s, %s);""",(83,27)),
        
        call("SELECT item_id FROM items WHERE item_name = 'Regular Filter coffee' LIMIT (1)"),
        call("""INSERT INTO transaction_items(transaction_id, item_id)
                VALUES (%s, %s);""",(83,52)),

        call("SELECT item_id FROM items WHERE item_name = 'Large Iced americano' LIMIT (1)"),
        call("""INSERT INTO transaction_items(transaction_id, item_id)
                VALUES (%s, %s);""",(83,28))
        
    ])

    connection.commit.assert_called_with()
    assert connection.commit.call_count == 7