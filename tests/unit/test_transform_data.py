
"""Unit tests for transform_data ETL logic."""
from pathlib import Path
import pytest
import polars as pl
from etl.transform_data import transform_products

data_cases = [
    # Normal case
    [{"price": 120, "category": "electronics"}, 100, 120, "high", "Electronics"],
    # Below threshold
    [{"price": 80, "category": "clothing"}, 100, 80, "normal", "Clothing"],
    # Edge: threshold
    [{"price": 100, "category": "books"}, 100, 100, "normal", "Books"],
    # Edge: empty category
    [{"price": 150, "category": ""}, 100, 150, "high", ""],
]

@pytest.mark.parametrize("product, threshold, price, flag, category", [
    (case[0], case[1], case[2], case[3], case[4]) for case in data_cases
])
def test_transform_products(
    tmp_path: Path,
    product: dict,
    threshold: float,
    price: float,
    flag: str,
    category: str,
    monkeypatch: pytest.MonkeyPatch
):
    """Test transform_products for expected output and edge cases."""
    """Test transform_products for expected output and edge cases."""
    # Mock exchange rate to 1 for simplicity
    monkeypatch.setattr("etl.transform_data.get_usd_to_gbp_rate", lambda: 1)
    # Ensure all required fields are present
    product = {
        "id": product.get("id", 1),
        "name": product.get("name", "Test Product"),
        **product
    }
    df = pl.DataFrame([product])
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "cleaned.parquet"
    df.write_ndjson(str(raw_path))
    transform_products(str(raw_path), str(out_path), price_threshold=threshold)
    result = pl.read_parquet(str(out_path))
    assert result["price_gbp"].to_list()[0] == price
    assert result["price_flag"].to_list()[0] == flag
    assert result["category"].to_list()[0] == category
