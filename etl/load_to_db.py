import logging
import polars as pl
import duckdb
import os

logging.basicConfig(level=logging.INFO)

PARQUET_PATH = "/opt/airflow/data/products_cleaned.parquet"
DB_PATH = "/opt/airflow/data/products.duckdb"

def load_to_db(parquet_path: str = PARQUET_PATH, db_path: str = DB_PATH) -> None:
    """Load processed product data from Parquet into DuckDB."""
    logging.info("Reading Parquet file from %s", parquet_path)
    df = pl.read_parquet(parquet_path)
    # Ensure parent directory for DuckDB exists (should always exist with volume mount)
    con = duckdb.connect(db_path)
    # Set id as INTEGER, others as VARCHAR
    col_defs = ', '.join([
        f'{name} INTEGER' if name == 'id' else f'{name} VARCHAR' for name in df.columns
    ])
    con.execute(f"CREATE TABLE IF NOT EXISTS products ({col_defs})")
    df_pd = df.to_pandas()
    con.register('df_pd', df_pd)
    con.execute("INSERT INTO products SELECT * FROM df_pd")
    con.close()
    logging.info("Appended cleaned data into DuckDB at %s", db_path)

if __name__ == "__main__":
    load_to_db()
