# Concept, Assumptions and Enhancements


## Concept

The application emulates the ETL process with a focus on the Transform and Load phases, as the extraction phase has already been completed and the data is available in raw CSV files.

### Core components of run_etl.py

1. **Setup Class**
    - **Purpose:** Initializes the DuckDB database and populates it with reference data, simulating a complete data environment within the database.
    - **Key Actions:**
        - Creates the `financial_data` DuckDB database.
        - Populates reference tables necessary for data integrity and relational operations.

2. **Transform Class**
    - **Purpose:** Cleans and processes raw CSV files to prepare them for loading into the database.
    - **Key Transformations:**
        - **Data Cleaning:** Removes inconsistencies and ensures data quality.
        - **Appending `data_date`:** Extracts the date from the CSV file name and appends it to the table schema.
        - **Snake Case Conversion:** Standardizes column names to snake_case for consistency and ease of use.
        - **Consolidation of Identifiers:** Merges SEDOL and ISIN codes into unified instrument identifiers to streamline data referencing.

3. **Load Class**
    - **Purpose:** Imports the transformed data into the DuckDB database in an efficient and idempotent manner.
    - **Key Actions:**
        - **Create or Replace Tables:** Ensures that tables are updated with the latest data without duplication.
        - **Data Ingestion:** Loads the transformed CSV files into the corresponding tables within the `financial_data` database.
        - **Idempotency:** Guarantees that the ETL process can be rerun without altering the final state, maintaining data integrity.

## Assumptions

- **Consistent File Naming:** Funds consistently provide CSV files with dates embedded in the file names.
- **Date Formats:** Dates within file names may follow various conventions.
- **File Naming Pattern:** CSV files, present and future, follow the naming pattern `<fund_name>.<date_in_various_formats><optional_description>.csv`.

## Enhancements

### Python Enhancements

- **Logging:** Implement logging over print statements to track the ETL process and facilitate debugging.
- **Testing:** Expand test coverage with additional negative and edge case scenarios to ensure robustness.

### Data Modeling Enhancements

- **Denormalization:**
    - Consolidate multiple tables into a single table with an added `source/fund` column due to identical schema.
    - Enhances scalability, allowing reconciliation and fund performance queries to efficiently handle an increasing number of fund reports.

	
# Setup

> **Prerequisites:**  
> - Python `^3.11.9`  
> - (If using Poetry) Poetry `^2.0.0`


**Installing dependencies:**

a. Using Pip

```bash
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```
b. Using Poetry

```bash
poetry env activate
poetry install
```

# Usage

Run the ETL pipeline
```bash
python run_etl.py
```

To generate reconciliation report
```bash
python insights.py ./queries/recon_query.sql
```
To generate fund performance report
```bash
python insights.py ./queries/fund_performance_query.sql
```

# Tests

```bash
pytest --cov-report=term
```

# Requirements

1. Generate a price reconciliation report that shows the difference between `the price on EOM date` from the reference data vs `the price` in the fund report. The report output should be in Excel. Please use python and leverage on data manipulation libraries like pandas/polars etc.
If the price is unavailable in reference db please use the last available price.
	a. show the break with instrument date ref price vs fund price.
	b. describe how can make it scalable for `N` number of fund reports in future

2. Based on your understanding of the data, attempt to create an Excel report which gives the best performing fund (highest raet of return) for every month. You can use the formula 
Rate of Return = (Fund_MV_end - Fund_MV_start + Realized P/L) / Fund_MV_start
Fund Market Value = Sum of (Symbol Market Value) for all symbols in that fund.
Fund Realized P/L = Sum of (Symbol Realized P/L) for the same monthly period.


