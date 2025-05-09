import pytest
from unittest.mock import MagicMock, mock_open, patch
from src.setup import Setup


@pytest.fixture
def setup_instance():
    """Fixture to create a Setup instance."""
    return Setup(db_file="test.db", sql_file="test.sql")


def test_connect_to_db(setup_instance):
    """Test the connect_to_db method."""
    with patch("duckdb.connect", return_value=MagicMock()) as mock_connect:
        setup_instance.connect_to_db()
        mock_connect.assert_called_once_with(database="test.db", read_only=False)
        assert setup_instance.conn is not None


def test_execute_sql_no_connection(setup_instance):
    """Test execute_sql raises an error if no connection is established."""
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        setup_instance.execute_sql()


def test_execute_sql(setup_instance):
    """Test the execute_sql method."""
    setup_instance.conn = MagicMock()
    sql_query = "SELECT * FROM test_table;"

    with patch("builtins.open", mock_open(read_data=sql_query)):
        setup_instance.execute_sql()
        setup_instance.conn.execute.assert_called_once_with(sql_query)
