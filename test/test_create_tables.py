# from src.create_database import create_locations_table, create_items_table, create_payment_types_table, create_transaction_table, create_transaction_items_table
# from unittest.mock import call, MagicMock

# def test_create_locations_table():
    
#     connection = MagicMock()

#     create_locations_table(connection)

#     connection.cursor().execute.assert_called_with("CREATE TABLE IF NOT EXISTS locations (\
#             location_id SERIAL PRIMARY KEY,\
#             location_name VARCHAR(200));")
#     connection.commit.assert_called_once()

    
# def test_create_items_table():
    
#     connection = MagicMock()

#     create_items_table(connection)

#     connection.cursor().execute.assert_called_with("""CREATE TABLE IF NOT EXISTS items(
#             item_id SERIAL PRIMARY KEY,
#             item_name VARCHAR(200),
#             item_price DECIMAL(19,2));
#         """)
#     connection.commit.assert_called_once()


# def test_create_payment_types_table():
    
#     connection = MagicMock()

#     create_payment_types_table(connection)

#     connection.cursor().execute.assert_has_calls([
#         call("CREATE TABLE IF NOT EXISTS payment_types (\
#             payment_id SERIAL PRIMARY KEY,\
#             payment VARCHAR(5));"),
#         call("INSERT INTO payment_types \
#                         VALUES (1, 'CARD'), (2, 'CASH') ON CONFLICT DO NOTHING;")
#             ])
#     connection.commit.assert_called_once()


# def test_create_transaction_table():
    
#     connection = MagicMock()

#     create_transaction_table(connection)

#     connection.cursor().execute.assert_called_with("""CREATE TABLE IF NOT EXISTS transactions (
#             transaction_id SERIAL PRIMARY KEY,
#             date_time TIMESTAMP, 
#             location_id INT,
#             total_price DECIMAL(19,2), 
#             payment_id INT, 
#             FOREIGN KEY (location_id) REFERENCES locations(location_id),
#             FOREIGN KEY (payment_id) REFERENCES payment_types(payment_id));
#             """)
#     connection.commit.assert_called_once()


# def test_create_transaction_items_table():
    
#     connection = MagicMock()

#     create_transaction_items_table(connection)

#     connection.cursor().execute.assert_called_with("""CREATE TABLE IF NOT EXISTS transaction_items (
#             transaction_id INT,
#             item_id INT,
#             FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
#             FOREIGN KEY (item_id) REFERENCES items(item_id));
#             """)
#     connection.commit.assert_called_once()
        