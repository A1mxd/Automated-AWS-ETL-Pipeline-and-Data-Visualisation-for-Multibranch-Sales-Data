from unittest.mock import MagicMock
from src.load_database import check_if_duplicate_entry

def test_check_if_duplicate_entry_found():

    connection = MagicMock()
    table_name = "locations"
    entry = "chesterfield"
    column_name = "location_name"

    connection.cursor().fetchone.side_effect = [(1,"london")]

    actual = check_if_duplicate_entry(connection, table_name, entry, column_name)
    expected = True

    assert actual == expected
    connection.cursor().execute.assert_called_with("SELECT * FROM locations WHERE location_name = 'chesterfield' LIMIT (1)")


def test_check_if_duplicate_entry_not_found():
    
    connection = MagicMock()
    table_name = "locations"
    entry = "london"
    column_name = "location_name"

    connection.cursor().fetchone.side_effect = [None]

    actual = check_if_duplicate_entry(connection, table_name, entry, column_name)
    expected = False

    assert actual == expected
    connection.cursor().execute.assert_called_with("SELECT * FROM locations WHERE location_name = 'london' LIMIT (1)")