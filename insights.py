import duckdb
from pathlib import Path
import sys


def get_csv_from_query():
    if len(sys.argv) != 2:
        print("Usage: python insights.py <txt or sql file>")
        return

    input_file = sys.argv[1]
    sql_file = Path(input_file)

    # Check if the file exists
    if not sql_file.exists() or not sql_file.is_file():
        print(f"Error: File '{sql_file}' does not exist.")
        return

    # Read the SQL query from the file
    with open(sql_file, "r", encoding="UTF-8") as file:
        sql_query = file.read().strip()

    db_file = Path("financial_data.duckdb")
    conn = duckdb.connect(database=str(db_file), read_only=True)

    try:
        print("\nExecuting query...")
        result_df = conn.execute(sql_query).df()

        # Show a preview of the result
        print("\nQuery executed successfully. Preview of the result:")
        print(result_df.head())

        # Determine the output CSV file name and path
        output_dir = Path("./query_output")
        output_dir.mkdir(exist_ok=True)
        output_csv_name = f"{sql_file.stem}_result.csv"
        output_csv_path = output_dir / output_csv_name

        # Write the result to a CSV file
        result_df.to_csv(output_csv_path, index=False)
        print(f"\nQuery result written to '{output_csv_path}'")

    except Exception as e:
        print(f"Error executing query: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    get_csv_from_query()
