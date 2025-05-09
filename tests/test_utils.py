import tempfile
from pathlib import Path
from typing import Optional

import pytest

from src.models.models import Config
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


def test_to_snake_case():
    """
    Test the to_snake_case function with various inputs.
    """
    assert ETLUtils.to_snake_case("Applebead") == "applebead"
    assert ETLUtils.to_snake_case("Fund Whitestone") == "fund_whitestone"
    assert ETLUtils.to_snake_case("mend-report Wallington") == "mend_report_wallington"
    assert ETLUtils.to_snake_case("Report-of-Gohen") == "report_of_gohen"
    assert ETLUtils.to_snake_case("Virtous") == "virtous"
    assert ETLUtils.to_snake_case("TT_monthly_Trustmind") == "tt_monthly_trustmind"


@pytest.mark.parametrize(
    "filename,expected_table_name",
    [
        ("Applebead.30-06-2023 breakdown.csv", "applebead"),
        ("Belaware.30_04_2023.csv", "belaware"),
        ("Fund Whitestone.30-09-2022 - details.csv", "fund_whitestone"),
        ("Leeder.04_30_2023.csv", "leeder"),
        ("Magnum.30-09-2022.csv", "magnum"),
        ("mend-report Wallington.30_04_2023.csv", "mend_report_wallington"),
        ("Report-of-Gohen.02-28-2023.csv", "report_of_gohen"),
        ("rpt-Catalysm.2023-03-31.csv", "rpt_catalysm"),
        ("TT_monthly_Trustmind.20220930.csv", "tt_monthly_trustmind"),
        ("Virtous.05-31-2023 - securities.csv", "virtous"),
        ("NoDateFile.csv", "no_date_file"),
        ("InvalidDate.99-99-9999.csv", "invalid_date"),
    ],
)
def test_extract_table_name(
    temp_directories, filename: str, expected_table_name: Optional[str]
):
    """
    Test the extract_table_name function with various filename patterns.
    """
    _, _ = temp_directories
    table_name = ETLUtils.extract_table_name(filename)
    assert table_name == expected_table_name, f"Failed for filename: {filename}"


@pytest.mark.parametrize(
    "filename,expected_date",
    [
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
        ("NoDateFile.csv", None),
        ("InvalidDate.99-99-9999.csv", None),
    ],
)
def test_extract_date(temp_directories, filename: str, expected_date: Optional[str]):
    """
    Test the extract_date function with various filename patterns.
    """
    input_dir, output_dir = temp_directories
    config = Config(input_directory=input_dir, output_directory=output_dir)
    extracted_date = ETLUtils.extract_date(
        filename, config.date_patterns, config.date_format
    )
    assert extracted_date == expected_date, f"Failed for filename: {filename}"
