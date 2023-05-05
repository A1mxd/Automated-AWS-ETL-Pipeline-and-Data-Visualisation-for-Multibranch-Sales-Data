import unittest
from unittest.mock import patch, MagicMock
from src.load_database import insert_into_location_table

@patch('src.load_database.check_if_duplicate_entry', return_value=False)
def test_inserts_new_location(mock_check_if_duplicate_entry):
    
    connection = MagicMock()
    unique_location_list = ['London']

    insert_into_location_table(connection, unique_location_list)
    
    connection.cursor().execute.assert_called_with(''' INSERT INTO locations(location_name)
                VALUES (%s);
                ''', ('London',))
    connection.commit.assert_called_once()
    
@patch('src.load_database.check_if_duplicate_entry', return_value=True)
def test_skips_existing_location(mock_check_if_duplicate_entry):

    connection = MagicMock()
    unique_location_list = ['Chesterfield', 'Leeds']

    insert_into_location_table(connection, unique_location_list)
    
    connection.cursor().execute.assert_not_called()
    connection.commit.assert_not_called()