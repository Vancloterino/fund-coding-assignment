import os
from pathlib import Path

import polars as pl

from src.models.models import Config
from src.utils.utils import ETLUtils
from src.config.constants import FileDirectoryPath


class Transform:

    @staticmethod
    def clean_csv_data(
        filename: str, file_path: os.PathLike, output_directory: os.PathLike, date: str
    ) -> None:
        """
        Appends the DATA_DATE column to the CSV file, converts column names to snake_case in caps,
        consolidates instrument identifiers and writes it to the output directory.

        Args:
            file_path (os.PathLike): The path to the original CSV file.
            output_directory (os.PathLike): The path to the output directory.
            date (str): The date string to append.
        """
        try:
            # Read the CSV file with Polars
            df = pl.read_csv(file_path)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return

        table_name = ETLUtils.extract_table_name(filename)

        df = df.rename({col: ETLUtils.to_snake_case(col).upper() for col in df.columns})

        # Consolidate instrument identifiers and rename to INST_ID
        df = df.rename(
            {col: "INST_ID" for col in df.columns if col in {"SEDOL", "ISIN"}}
        )

        # Insert the DATA_DATE column at the beginning
        df = df.with_columns([pl.lit(date).alias("DATA_DATE")])
        df = df.with_columns([pl.lit(table_name).alias("SOURCE")])

        # Reorder columns to have DATA_DATE first
        cols = df.columns
        cols = ["DATA_DATE"] + [col for col in cols if col != "DATA_DATE"]
        df = df.select(cols)

        # Determine the output file path
        filename = os.path.basename(file_path)
        output_path = os.path.join(output_directory, filename)

        try:
            df.write_csv(output_path)
            print(f"Created {output_path} with DATA_DATE {date}")
        except Exception as e:
            print(f"Error writing to {output_path}: {e}")

    @staticmethod
    def process_files(config: Config) -> None:
        """
        Processes all CSV files in the input directory by appending the DATA_DATE column
        and writing the updated files to the output directory.

        Args:
            config (Config): Configuration settings.
        """
        for filename in os.listdir(config.input_directory):
            if filename.lower().endswith(".csv"):
                file_path = os.path.join(config.input_directory, filename)
                date = ETLUtils.extract_date(
                    filename, config.date_patterns, config.date_format
                )

                if date:
                    Transform.clean_csv_data(
                        filename, file_path, config.output_directory, date
                    )
                else:
                    print(f"No valid date found in filename: {filename}")

    @staticmethod
    def transform_step() -> None:
        """
        Main function to execute the script.
        """
        try:
            config = Config(
                input_directory=Path(FileDirectoryPath.EXTERNAL_FUNDS_CSV.value),
                output_directory=Path(
                    FileDirectoryPath.EXTERNAL_FUNDS_CSV_TRANSFORMED.value
                ),
            )
            Transform.process_files(config)
        except Exception as e:
            print(f"Error encountered in transform_step : {e}")
