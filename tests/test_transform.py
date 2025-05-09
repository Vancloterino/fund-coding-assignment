import tempfile
from pathlib import Path
from typing import Optional

import pytest
import polars as pl

from src.models.models import Config
from src.transform import Transform
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
    Pytest fixture to provide sample CSV content with SEDOL and ISIN columns.
    """
    return r"""FINANCIAL TYPE,SYMBOL,SECURITY NAME,ISIN,PRICE,QUANTITY,REALISED P/L,MARKET VALUE
            Equity,AAPL,Apple Inc.,US0378331005,150.00,10,500.00,1500.00
            Equity,GOOGL,Alphabet Inc.,US02079K3059,2800.00,5,14000.00,14000.00
            """.strip()


@pytest.mark.parametrize(
    "filename,expected_table_name,expected_date",
    [
        ("Applebead.30-06-2023 breakdown.csv", "applebead", "2023-06-30"),
        ("Belaware.30_04_2023.csv", "belaware", "2023-04-30"),
        ("Fund Whitestone.30-09-2022 - details.csv", "fund_whitestone", "2022-09-30"),
        ("Leeder.04_30_2023.csv", "leeder", "2023-04-30"),
        ("Magnum.30-09-2022.csv", "magnum", "2022-09-30"),
        (
            "mend-report Wallington.30_04_2023.csv",
            "mend_report_wallington",
            "2023-04-30",
        ),
        ("Report-of-Gohen.02-28-2023.csv", "report_of_gohen", "2023-02-28"),
        ("rpt-Catalysm.2023-03-31.csv", "rpt_catalysm", "2023-03-31"),
        ("TT_monthly_Trustmind.20220930.csv", "tt_monthly_trustmind", "2022-09-30"),
        ("Virtous.05-31-2023 - securities.csv", "virtous", "2023-05-31"),
        ("NoDateFile.csv", "no_date_file", None),
        ("InvalidDate.99-99-9999.csv", "invalid_date", None),
    ],
)
def test_extract_table_name_and_date(
    temp_directories,
    filename: str,
    expected_table_name: Optional[str],
    expected_date: Optional[str],
):
    """
    Test the Transform.extract_table_name and ETLUtils.extract_date functions with various filename patterns.
    Ensures table names are in snake_case and dates are correctly extracted.
    """
    input_dir, output_dir = temp_directories
    config = Config(input_directory=input_dir, output_directory=output_dir)

    # Extract table name using Transform class
    extracted_table_name = ETLUtils.extract_table_name(filename)
    assert (
        extracted_table_name == expected_table_name
    ), f"Failed table name extraction for filename: {filename}"

    # Extract date using ETLUtils class
    extracted_date = ETLUtils.extract_date(
        filename, config.date_patterns, config.date_format
    )
    assert (
        extracted_date == expected_date
    ), f"Failed date extraction for filename: {filename}"


def test_clean_csv_data(temp_directories, sample_csv_content):
    """
    Test the Transform.clean_csv_data function to ensure DATA_DATE is appended correctly,
    columns are renamed to snake_case and uppercase, 'SEDOL' and 'ISIN' are renamed to 'INST_ID',
    and 'SOURCE' column is added with correct table name.
    """
    input_dir, output_dir = temp_directories
    filename = "Applebead.30-06-2023 breakdown.csv"
    file_path = input_dir / filename
    date = "2023-06-30"

    # Write sample CSV content to input file
    file_path.write_text(sample_csv_content)

    # Call the clean_csv_data function
    Transform.clean_csv_data(
        filename=filename,
        file_path=str(file_path),
        output_directory=str(output_dir),
        date=date,
    )

    # Check that the output file exists
    output_file = output_dir / filename
    assert output_file.exists(), f"Output file {filename} was not created."

    # Read the transformed CSV
    df = pl.read_csv(output_file)

    # Check for 'DATA_DATE' column with correct date
    assert "DATA_DATE" in df.columns, "DATA_DATE column is missing."
    assert all(df["DATA_DATE"] == date), "DATA_DATE values are incorrect."

    # Check for 'SOURCE' column with correct table name (snake_case)
    expected_table_name = ETLUtils.to_snake_case(ETLUtils.extract_table_name(filename))
    assert "SOURCE" in df.columns, "SOURCE column is missing."
    assert all(df["SOURCE"] == expected_table_name), "SOURCE values are incorrect."

    # Check that 'SEDOL' and 'ISIN' are replaced with 'INST_ID'
    assert "INST_ID" in df.columns, "INST_ID column is missing."
    assert (
        "SEDOL" not in df.columns
    ), "SEDOL column should have been renamed to INST_ID."
    assert "ISIN" not in df.columns, "ISIN column should have been renamed to INST_ID."

    expected_columns = [
        "DATA_DATE",
        "FINANCIAL_TYPE",
        "SYMBOL",
        "SECURITY_NAME",
        "INST_ID",
        "PRICE",
        "QUANTITY",
        "REALISED_PL",
        "MARKET_VALUE",
        "SOURCE",
    ]

    # Verify that all expected columns are present
    assert set(df.columns) == set(
        expected_columns
    ), f"Columns in output CSV do not match expected columns. Found: {df.columns}, Expected: {expected_columns}"


def test_process_files(temp_directories, sample_csv_content):
    """
    Test the Transform.process_files function to ensure it processes multiple CSV files correctly.
    Ensures that only files with valid dates are processed and transformed.
    """
    input_dir, output_dir = temp_directories

    config = Config(input_directory=input_dir, output_directory=output_dir)

    # Define multiple test files with corresponding expected dates
    files_info = [
        ("Applebead.30-06-2023 breakdown.csv", "2023-06-30"),
        ("Belaware.30_04_2023.csv", "2023-04-30"),
        ("NoDateFile.csv", None),
        ("InvalidDate.99-99-9999.csv", None),
    ]

    # Create and write sample CSV files
    for filename, date in files_info:
        file_path = input_dir / filename
        if date:
            file_path.write_text(sample_csv_content)
        else:
            file_path.write_text(
                "FINANCIAL TYPE,SYMBOL,SECURITY NAME,PRICE,QUANTITY,REALISED P/L,MARKET VALUE\n"
            )

    # Call the process_files function
    Transform.process_files(config)

    # Now, verify that only files with valid dates are processed
    for filename, expected_date in files_info:
        output_file = output_dir / filename
        if expected_date:
            # Output file should exist
            assert output_file.exists(), f"Output file {filename} was not created."
            df = pl.read_csv(output_file)

            # Check 'DATA_DATE' column
            assert "DATA_DATE" in df.columns, "DATA_DATE column is missing."
            assert all(
                df["DATA_DATE"] == expected_date
            ), "DATA_DATE values are incorrect."

            # Check 'SOURCE' column
            expected_table_name = ETLUtils.to_snake_case(
                ETLUtils.extract_table_name(filename)
            )
            assert "SOURCE" in df.columns, "SOURCE column is missing."
            assert all(
                df["SOURCE"] == expected_table_name
            ), "SOURCE values are incorrect."

            # Check 'INST_ID' column
            assert "INST_ID" in df.columns, "INST_ID column is missing."
            assert (
                "SEDOL" not in df.columns
            ), "SEDOL column should have been renamed to INST_ID."
            assert (
                "ISIN" not in df.columns
            ), "ISIN column should have been renamed to INST_ID."

            # Check other columns have been converted to snake_case and uppercase
            expected_columns = [
                "DATA_DATE",
                "FINANCIAL_TYPE",
                "SYMBOL",
                "SECURITY_NAME",
                "INST_ID",
                "PRICE",
                "QUANTITY",
                "REALISED_PL",
                "MARKET_VALUE",
                "SOURCE",
            ]

            # Verify that all expected columns are present
            assert set(df.columns) == set(expected_columns), (
                f"Columns in output CSV do not match expected columns for {filename}. "
                f"Found: {df.columns}, Expected: {expected_columns}"
            )
        else:
            # Output file should not exist
            assert (
                not output_file.exists()
            ), f"File without date {filename} should not be created."
