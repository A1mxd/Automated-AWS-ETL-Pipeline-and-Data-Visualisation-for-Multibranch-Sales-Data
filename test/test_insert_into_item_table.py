# from unittest.mock import call, MagicMock
# from src.load_database import insert_into_item_table


# def test_inserts_new_item():
    
#     connection = MagicMock()
#     connection.cursor().fetchone.side_effect = [None]

#     unique_items_list = [{"item_name": "coffee",
#                           "item_price": 1.35,
#                           "temp_transaction_id": 1}]

#     insert_into_item_table(connection, unique_items_list)

#     connection.cursor().execute.assert_has_calls([
#         call('''SELECT * FROM items WHERE item_name = 'coffee' AND item_price = '1.35' LIMIT (1);'''),
#         call(''' INSERT INTO items(item_name, item_price)
#                 VALUES (%s, %s);
#                 ''', ('coffee', 1.35))])
#     connection.commit.assert_called_once()
    

# def test_skips_existing_item():

#     connection = MagicMock()
#     connection.cursor().fetchone.side_effect = [(1, "hot chocolate",2.35),(2, "tea",1.85)]

#     unique_items_list = [{"item_name": "hot chocolate",
#                           "item_price": 2.35,
#                           "temp_transaction_id": 1},
#                           {"item_name": "tea",
#                           "item_price": 1.85,
#                           "temp_transaction_id": 2}]

#     insert_into_item_table(connection, unique_items_list)

#     connection.cursor().execute.assert_has_calls([
#         call('''SELECT * FROM items WHERE item_name = 'hot chocolate' AND item_price = '2.35' LIMIT (1);'''),
#         call('''SELECT * FROM items WHERE item_name = 'tea' AND item_price = '1.85' LIMIT (1);''')
#     ])
#     connection.commit.assert_not_called()


# def test_skips_existing_items_and_inserts_new_items():
#     connection = MagicMock()
#     connection.cursor().fetchone.side_effect = [(1, "hot chocolate",2.35),None,(2, "tea",1.85)]

#     unique_items_list = [{"item_name": "hot chocolate",
#                           "item_price": 2.35,
#                           "temp_transaction_id": 1},
#                           {"item_name": "coffee",
#                           "item_price": 1.35,
#                           "temp_transaction_id": 1},
#                           {"item_name": "tea",
#                           "item_price": 1.85,
#                           "temp_transaction_id": 2}]

#     insert_into_item_table(connection, unique_items_list)

#     connection.cursor().execute.assert_has_calls([
#         call('''SELECT * FROM items WHERE item_name = 'hot chocolate' AND item_price = '2.35' LIMIT (1);'''),
#         call('''SELECT * FROM items WHERE item_name = 'coffee' AND item_price = '1.35' LIMIT (1);'''),
#         call(''' INSERT INTO items(item_name, item_price)
#                 VALUES (%s, %s);
#                 ''', ('coffee', 1.35)),
#         call('''SELECT * FROM items WHERE item_name = 'tea' AND item_price = '1.85' LIMIT (1);''')])
        
#     connection.commit.assert_called_once()