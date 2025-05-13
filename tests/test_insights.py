import sys
import tempfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch


from insights import get_csv_from_query


@pytest.fixture
def temp_directories():
    """
    Pytest fixture to create temporary input and output directories.
    """
    with tempfile.TemporaryDirectory() as input_dir, tempfile.TemporaryDirectory() as output_dir:
        yield Path(input_dir), Path(output_dir)


@patch("duckdb.connect")
@patch("pandas.DataFrame.to_csv")
def test_insights_with_mock_duckdb(mock_to_csv, mock_connect, temp_directories):
    # Setup mock connection and cursor
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.df.return_value = pd.DataFrame(
        {"col1": [1, 2], "col2": ["a", "b"]}
    )

    # Write sample SQL file
    sql_file = temp_directories[0] / "query.sql"
    sql_file.write_text("SELECT * FROM some_table;")

    # Run the main function with the mocked duckdb
    with patch.object(sys, "argv", ["insights.py", str(sql_file)]):
        get_csv_from_query()

    # Assert calls
    mock_connect.assert_called_once()
    mock_conn.execute.assert_called_once_with("SELECT * FROM some_table;")
    mock_to_csv.assert_called_once()
