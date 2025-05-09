import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import duckdb


class ETLUtils:

    @staticmethod
    def to_snake_case(name: str) -> str:
        """
        Converts a given string to snake_case.

        Args:
            name (str): The string to convert.

        Returns:
            str: The converted snake_case string.
        """
        # Remove special characters
        name = re.sub(r"[^\w\s\-]", "", name)
        # Replace spaces and hyphens with underscores
        name = re.sub(r"[\s\-]+", "_", name)
        # Convert CamelCase or PascalCase to snake_case
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    @staticmethod
    def extract_table_name(filename: str) -> Optional[str]:
        """
        Extracts the table name from the filename and converts it to snake_case.
        The table name is the part before the first dot '.' in the filename.

        Args:
            filename (str): The name of the file.

        Returns:
            Optional[str]: The extracted snake_case table name or None if extraction fails.
        """
        match = re.match(r"^([^\.]+)\.", filename)
        if match:
            raw_table_name = match.group(1)
            return ETLUtils.to_snake_case(raw_table_name)
        return None

    @staticmethod
    def extract_date(
        filename: str, patterns: List[str], date_format: str
    ) -> Optional[str]:
        """
        Extracts and parses the date from the filename.

        Args:
            filename (str): The name of the file.
            patterns (List[str]): List of regex patterns to search for dates.
            date_format (str): The desired output date format.

        Returns:
            Optional[str]: The date in the specified format if found, else None.
        """
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                try:
                    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
                        # YYYY-MM-DD
                        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                    elif re.fullmatch(r"\d{2}-\d{2}-\d{4}", date_str):
                        # Determine if MM-DD-YYYY or DD-MM-YYYY based on the first value
                        first_part = int(date_str.split("-")[0])
                        if 1 <= first_part <= 12:
                            # Assume MM-DD-YYYY
                            parsed_date = datetime.strptime(date_str, "%m-%d-%Y")
                        else:
                            # Assume DD-MM-YYYY
                            parsed_date = datetime.strptime(date_str, "%d-%m-%Y")
                    elif re.fullmatch(r"\d{2}_\d{2}_\d{4}", date_str):
                        # Attempt MM_DD_YYYY
                        try:
                            parsed_date = datetime.strptime(date_str, "%m_%d_%Y")
                        except ValueError:
                            # Fallback to DD_MM_YYYY
                            parsed_date = datetime.strptime(date_str, "%d_%m_%Y")
                    elif re.fullmatch(r"\d{8}", date_str):
                        # YYYYMMDD
                        parsed_date = datetime.strptime(date_str, "%Y%m%d")
                    else:
                        # For any other unmatched patterns
                        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                    return parsed_date.strftime(date_format)
                except ValueError:
                    continue
        return None

    @staticmethod
    def initialize_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
        """
        Connects to the DuckDB database. Creates the database file if it doesn't exist.

        Args:
            db_path (Path): Path to the DuckDB database file.

        Returns:
            duckdb.DuckDBPyConnection: The DuckDB connection object.
        """
        conn = duckdb.connect(database=str(db_path), read_only=False)
        return conn
