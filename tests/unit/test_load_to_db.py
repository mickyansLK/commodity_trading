
"""Unit tests for load_to_db ETL logic."""
from pathlib import Path
import pytest
import polars as pl
import duckdb
from etl.load_to_db import load_to_db

def test_load_to_db_appends(tmp_path: Path):
    """Test that load_to_db appends data to DuckDB table."""
    """Test that load_to_db appends data to DuckDB table."""
    # Edge case parameters using list comprehension
    ids = [1, 2]
    names = ["A", "B"]
    df = pl.DataFrame({"id": ids, "name": names})
    parquet_path = tmp_path / "test.parquet"
    db_path = tmp_path / "test.duckdb"
    df.write_parquet(str(parquet_path))
    # First load
    load_to_db(str(parquet_path), str(db_path))
    # Second load (should append)
    load_to_db(str(parquet_path), str(db_path))
    con = duckdb.connect(str(db_path))
    result = con.execute("SELECT * FROM products").fetchall()
    con.close()
    expected = list(zip(ids, names)) * 2
    assert result == expected

def test_load_to_db_creates_table(tmp_path: Path):
    """Test that load_to_db creates DuckDB table if not exists."""
    """Test that load_to_db creates DuckDB table if not exists."""
    # Edge case: single row
    df = pl.DataFrame({"id": [3], "name": ["C"]})
    parquet_path = tmp_path / "test2.parquet"
    db_path = tmp_path / "test2.duckdb"
    df.write_parquet(str(parquet_path))
    load_to_db(str(parquet_path), str(db_path))
    con = duckdb.connect(str(db_path))
    tables = con.execute("SHOW TABLES").fetchall()
    con.close()
    assert ("products",) in tables

def test_load_to_db_empty_parquet(tmp_path: Path):
    """Test that load_to_db handles empty Parquet file."""
    """Test that load_to_db handles empty Parquet file."""
    # Edge case: empty DataFrame
    df = pl.DataFrame({"id": [], "name": []})
    parquet_path = tmp_path / "empty.parquet"
    db_path = tmp_path / "empty.duckdb"
    df.write_parquet(str(parquet_path))
    load_to_db(str(parquet_path), str(db_path))
    con = duckdb.connect(str(db_path))
    result = con.execute("SELECT * FROM products").fetchall()
    con.close()
    assert result == []
