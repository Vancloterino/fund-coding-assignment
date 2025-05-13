import os
from pathlib import Path

import duckdb

from src.utils.utils import ETLUtils
from src.config.constants import DatabaseContants, FileDirectoryPath


class Load:

    @staticmethod
    def create_or_replace_table(
        conn: duckdb.DuckDBPyConnection, table_name: str, csv_file: Path
    ) -> None:
        """
        Creates or replaces a table in DuckDB based on the CSV schema.

        Args:
            conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
            table_name (str): The name of the table to create or replace.
            csv_file (Path): Path to the CSV file.
        """
        print(f"Creating or replacing table '{table_name}' based on '{csv_file.name}'")
        create_table_query = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * 
            FROM read_csv_auto('{csv_file}')
            LIMIT 0
        """
        try:
            conn.execute(create_table_query)
            print(f"Table '{table_name}' created or replaced successfully.")
        except Exception as e:
            print(f"Error creating or replacing table '{table_name}': {e}")

    @staticmethod
    def ingest_csv_to_table(
        conn: duckdb.DuckDBPyConnection, table_name: str, csv_file: Path
    ) -> None:
        """
        Inserts data from the CSV file into the specified table.

        Args:
            conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
            table_name (str): The name of the table to insert data into.
            csv_file (Path): Path to the CSV file.
        """
        print(f"Inserting data from '{csv_file.name}' into table '{table_name}'")
        copy_query = f"""
            COPY {table_name} FROM '{csv_file}' 
            (FORMAT CSV, HEADER TRUE)
        """
        try:
            conn.execute(copy_query)
            print(f"Data inserted into table '{table_name}' successfully.\n")
        except Exception as e:
            print(f"Error inserting data into table '{table_name}': {e}\n")

    @staticmethod
    def process_files(config) -> None:
        """
        Processes all CSV files in the input directory by creating/replacing tables
        and inserting data into DuckDB.

        Args:
            config (Config): Configuration settings.
        """
        table_record = []
        for filename in os.listdir(config.get("input_directory")):
            if filename.lower().endswith(".csv"):
                file_path = os.path.join(config.get("input_directory"), filename)
                table_name = ETLUtils.extract_table_name(filename)

                if not table_name:
                    print(
                        f"Could not extract table name from '{filename}'. Skipping file.\n"
                    )
                    continue

                if table_name not in table_record:
                    Load.create_or_replace_table(
                        config.get("conn"), table_name, Path(file_path)
                    )
                    table_record.append(table_name)

                Load.ingest_csv_to_table(
                    config.get("conn"), table_name, Path(file_path)
                )

    @staticmethod
    def load_step() -> None:
        """
        Main function to execute the ingestion script.
        """
        # Define paths
        transformed_dir = Path(FileDirectoryPath.EXTERNAL_FUNDS_CSV_TRANSFORMED.value)
        db_file = Path(DatabaseContants.DATABASE_FILE.value)

        # Initialize DuckDB connection
        conn = ETLUtils.initialize_duckdb(db_file)

        # Define Config as a dictionary
        config = {
            "input_directory": transformed_dir,
            "conn": conn,
        }

        # Process the files
        Load.process_files(config)

        conn.close()
        print("All CSV files have been ingested successfully.")
