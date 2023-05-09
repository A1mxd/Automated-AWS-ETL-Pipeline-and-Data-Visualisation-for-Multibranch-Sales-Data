from unittest.mock import patch, MagicMock
from src.load_database import insert_into_item_table

@patch('connection.cursor.fetchone()', return_value=False)
def test_inserts_new_location(mock_check_if_duplicate_entry):
    
    connection = MagicMock()
    unique_items_list = [{"item_name": "coffee",
                          "item_price": 1.35,
                          "temp_transaction_id": 1}]

    insert_into_item_table(connection, unique_items_list)
    
    connection.cursor().execute.assert_called_with(''' INSERT INTO items(item_name,item_price)
                VALUES (%s);
                ''', ('coffee',1.35))
    connection.commit.assert_called_once()
    
@patch('connection.cursor.fetchone()', return_value=True)
def test_skips_existing_location(mock_check_if_duplicate_entry):

    connection = MagicMock()
    unique_items_list = [{"item_name": "hot chocolate",
                          "item_price": 2.35,
                          "temp_transaction_id": 1},
                          {"item_name": "tea",
                          "item_price": 1.85,
                          "temp_transaction_id": 2}]

    insert_into_item_table(connection, unique_items_list)
    
    connection.cursor().execute.assert_not_called()
    connection.commit.assert_not_called()