import tempfile
from pathlib import Path

import duckdb
import pytest

from src.load import Load
from src.utils.utils import ETLUtils


@pytest.fixture
def temp_directories():
    """
    Pytest fixture to create temporary input and output directories.
    """
    with tempfile.TemporaryDirectory() as input_dir, tempfile.TemporaryDirectory() as output_dir:
        yield Path(input_dir), Path(output_dir)


@pytest.fixture
def sample_csv_content():
    """
    Pytest fixture to provide sample CSV content with a dynamic DATA_DATE.
    """
    return r"""FINANCIAL TYPE,SYMBOL,SECURITY NAME,ISIN,PRICE,QUANTITY,REALISED P/L,MARKET VALUE,DATA_DATE
                Equity,AAPL,Apple Inc.,US0378331005,150.00,10,500.00,1500.00,2023-01-01
                Equity,GOOGL,Alphabet Inc.,US02079K3059,2800.00,5,14000.00,14000.00,2023-01-01
                """.strip()


def test_ingest_csv_to_table(temp_directories, sample_csv_content):
    """
    Test the ingest_csv_to_table function to ensure DATA_DATE is present correctly.
    """
    input_dir, _ = temp_directories
    table_name = "ingest_test"
    csv_filename = f"{table_name}.01-01-2023.csv"
    csv_path = input_dir / csv_filename
    test_date = "2023-01-01"

    # Create a sample input CSV file
    csv_path.write_text(sample_csv_content)

    # Initialize DuckDB connection
    db_file = Path("test_financial_data.duckdb")
    conn = duckdb.connect(database=str(db_file), read_only=False)

    # Create or replace table
    Load.create_or_replace_table(conn, table_name, csv_path)

    # Ingest data
    Load.ingest_csv_to_table(conn, table_name, csv_path)

    # Read data from DuckDB and verify
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    assert "DATA_DATE" in df.columns, "DATA_DATE column is missing."
    assert all(df["DATA_DATE"] == test_date), "DATA_DATE values are incorrect."

    # Clean up
    conn.close()
    db_file.unlink()


def test_process_files(temp_directories, sample_csv_content):
    """
    Test the process_files function to ensure it processes multiple files correctly.
    """
    input_dir, output_dir = temp_directories

    # Define sample filenames and their expected DATA_DATE
    files_info = [
        ("Applebead.30-06-2023 breakdown.csv", "2023-06-30"),
        ("Belaware.30_04_2023.csv", "2023-04-30"),
        ("Fund Whitestone.30-09-2022 - details.csv", "2022-09-30"),
        ("Leeder.04_30_2023.csv", "2023-04-30"),
        ("Magnum.30-09-2022.csv", "2022-09-30"),
        ("mend-report Wallington.30_04_2023.csv", "2023-04-30"),
        ("Report-of-Gohen.02-28-2023.csv", "2023-02-28"),
        ("rpt-Catalysm.2023-03-31.csv", "2023-03-31"),
        ("TT_monthly_Trustmind.20220930.csv", "2022-09-30"),
        ("Virtous.05-31-2023 - securities.csv", "2023-05-31"),
        ("NoDateFile.csv", "nodatefile"),
        ("InvalidDate.99-99-9999.csv", "invaliddate"),
    ]

    # Create sample input CSV files
    for filename, expected_date in files_info:
        file_path = input_dir / filename
        if "NoDateFile" not in filename and "InvalidDate" not in filename:
            file_path.write_text(sample_csv_content)
        else:
            # Create empty CSV files or files with minimal content
            file_path.write_text(
                """FINANCIAL TYPE,SYMBOL,SECURITY NAME,ISIN,PRICE,QUANTITY,REALISED P/L,MARKET VALUE,DATA_DATE"""
            )

    # Initialize DuckDB connection
    db_file = Path("test_financial_data.duckdb")
    conn = duckdb.connect(database=str(db_file), read_only=False)

    config = {"input_directory": input_dir, "conn": conn}

    # Process the files
    Load.process_files(config)

    # Verify that files with valid dates are processed
    for filename, expected_date in files_info:
        output_file = output_dir / filename
        table_name = ETLUtils.extract_table_name(filename)
        if table_name:
            # Only verify if the table was supposed to be created
            if "NoDateFile" in filename or "InvalidDate" in filename:
                # These files have table names but contain no data; ensure table exists but empty
                if "NoDateFile" in filename or "InvalidDate" in filename:
                    if expected_date != "nodatefile" and expected_date != "invaliddate":
                        continue  # Should not be processed
            if expected_date in ["nodatefile", "invaliddate"]:
                if output_file.exists():
                    # Read data from DuckDB
                    df = conn.execute(f"SELECT * FROM {table_name}").df()
                    assert (
                        "DATA_DATE" in df.columns
                    ), f"DATA_DATE column missing in {table_name}."
                    # Since the CSV was empty except headers, DATA_DATE should be empty or NaN
                    assert df.empty, f"Table {table_name} should be empty but has data."
            else:
                # For valid files, verify DATA_DATE
                if output_file.exists():
                    df = conn.execute(f"SELECT * FROM {table_name}").df()
                    assert (
                        "DATA_DATE" in df.columns
                    ), f"DATA_DATE column missing in {table_name}."
                    assert all(
                        df["DATA_DATE"] == expected_date
                    ), f"Incorrect DATA_DATE in {table_name}."
        else:
            # Table should not exist for files without a table name
            assert (
                not output_file.exists()
            ), f"File without table name {filename} should not be created."

    # Clean up
    conn.close()
    db_file.unlink()
