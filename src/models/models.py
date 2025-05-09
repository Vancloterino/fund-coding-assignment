from pydantic import BaseModel, DirectoryPath, Field
from typing import List


class Config(BaseModel):
    """
    Configuration model using Pydantic for managing script settings.
    """

    input_directory: DirectoryPath = Field(
        ..., description="Path to the directory containing the original CSV files."
    )
    output_directory: DirectoryPath = Field(
        ..., description="Path to the directory where updated CSV files will be saved."
    )
    date_patterns: List[str] = Field(
        default_factory=lambda: [
            r"(\d{4}-\d{2}-\d{2})",  # e.g., 2023-03-31
            r"(\d{2}-\d{2}-\d{4})",  # e.g., 30-06-2023 or 05-31-2023
            r"(\d{2}_\d{2}_\d{4})",  # e.g., 30_04_2023 or 04_30_2023
            r"(\d{8})",  # e.g., 20220930
        ],
        description="List of regex patterns to extract dates from filenames.",
    )
    date_format: str = Field(
        default="%Y-%m-%d",
        description="Standard date format to use in the DATA_DATE column.",
    )
