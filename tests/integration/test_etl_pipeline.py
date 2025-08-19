
"""Integration test for full ETL pipeline."""
from pathlib import Path
import pytest
import duckdb
from etl.fetch_data import fetch_products
from etl.transform_data import transform_products
from etl.load_to_db import load_to_db


@pytest.mark.integration
def test_etl_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test full ETL pipeline: fetch, transform, load, and validate DB."""
    # Mock exchange rate to 1 for deterministic test
    monkeypatch.setattr("etl.transform_data.get_usd_to_gbp_rate", lambda: 1)
    # Set up paths
    raw_path = tmp_path / "products_raw.json"
    cleaned_path = tmp_path / "products_cleaned.parquet"
    db_path = tmp_path / "products.duckdb"
    # Fetch data
    fetch_products(
        api_url="https://fakestoreapi.com/products",
        out_path=str(raw_path),
        output_format="json"
    )
    # Transform data
    transform_products(
        str(raw_path),
        str(cleaned_path),
        price_threshold=100
    )
    # Load to DB
    load_to_db(str(cleaned_path), str(db_path))
    # Validate DB contents
    con = duckdb.connect(str(db_path))
    result = con.execute("SELECT * FROM products").fetchall()
    con.close()
    assert len(result) > 0
    # Optionally, check column names
    con = duckdb.connect(str(db_path))
    columns = [desc[0] for desc in con.execute("SELECT * FROM products LIMIT 1").description]
    con.close()
    assert "price_gbp" in columns
    assert "price_flag" in columns
    assert "category" in columns
