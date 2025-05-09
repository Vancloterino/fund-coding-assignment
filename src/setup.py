from pathlib import Path
import duckdb
from src.config.constants import DatabaseContants, FileDirectoryPath


class Setup:
    def __init__(self, db_file: str, sql_file: str) -> None:
        self.db_file = Path(db_file)
        self.sql_file = Path(sql_file)
        self.conn = None

    def connect_to_db(self) -> None:
        """Establish a connection to the DuckDB database."""
        self.conn = duckdb.connect(database=str(self.db_file), read_only=False)

    def execute_sql(self) -> None:
        """Read and execute the SQL query from the file."""
        if not self.conn:
            raise ConnectionError("Database connection is not established.")
        try:
            with open(self.sql_file, "r", encoding="UTF-8") as file:
                sql_query = file.read()

            self.conn.execute(sql_query)
        finally:
            self.conn.close()

    @staticmethod
    def setup_step() -> None:
        """Execute the setup steps."""
        setup = Setup(
            db_file=DatabaseContants.DATABASE_FILE.value,
            sql_file=FileDirectoryPath.MASTER_REFERENCE_SQL.value,
        )
        setup.connect_to_db()
        setup.execute_sql()
