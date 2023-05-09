from unittest.mock import patch, MagicMock
from src.load_database import insert_into_item_table


def test_inserts_new_item():
    
    connection = MagicMock()
    # connection.cursor.fetchone = MagicMock()

    unique_items_list = [{"item_name": "coffee",
                          "item_price": 1.35,
                          "temp_transaction_id": 1}]

    connection.cursor.return_value.fetchone = None
    
    expected = connection.cursor().execute(' INSERT INTO items(item_name,item_price)\n                VALUES (%s);\n                ', ('coffee', 1.35))

    result = insert_into_item_table(connection, unique_items_list)


    assert expected == result
    # connection.commit.assert_called_once()


    # result = connection.cursor().execute.assert_called_with(''' INSERT INTO items(item_name,item_price)
    #             VALUES (%s, %s);
    #             ''', ('coffee',1.35))
    # if connection.cursor.fetchone:
    #     pass
    # else:
    # connection.cursor().execute.assert_called_with(''' INSERT INTO items(item_name,item_price)
    #             VALUES (%s);
    #             ''', ('coffee',1.35))

def test_skips_existing_item():

    connection = MagicMock()
    connection.cursor.fetchone = MagicMock()

    unique_items_list = [{"item_name": "hot chocolate",
                          "item_price": 2.35,
                          "temp_transaction_id": 1},
                          {"item_name": "tea",
                          "item_price": 1.85,
                          "temp_transaction_id": 2}]

    insert_into_item_table(connection, unique_items_list)
    
    connection.cursor().execute.assert_not_called()
    connection.commit.assert_not_called()