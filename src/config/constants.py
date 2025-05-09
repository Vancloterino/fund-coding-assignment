from enum import Enum


class DatabaseContants(Enum):
    DATABASE_FILE = "financial_data.duckdb"


class FileDirectoryPath(Enum):
    MASTER_REFERENCE_SQL = "./master-reference-sql.sql"
    EXTERNAL_FUNDS_CSV = "./external_funds"
    EXTERNAL_FUNDS_CSV_TRANSFORMED = "./external_funds_transformed"
