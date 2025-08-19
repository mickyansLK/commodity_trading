
"""Transform product data: cleaning, USD→GBP conversion, simple features."""
from __future__ import annotations
import os
import logging
from pathlib import Path
from typing import Optional

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Absolute, container-safe paths (override with DATA_DIR env)
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
RAW_DEFAULT = DATA_DIR / "products_raw.parquet"
OUT_DEFAULT = DATA_DIR / "products_cleaned.parquet"

REQUIRED_COLS = {"id", "name", "price", "category"}  # assumes input prices are USD

def _requests_session_with_retries(total: int = 3, backoff: float = 0.5) -> requests.Session:
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def get_usd_to_gbp_rate(app_id: Optional[str] = None, timeout: float = 10.0) -> float:
    """
    Fetch USD→GBP FX rate from Open Exchange Rates.
    Requires OXR_APP_ID in env or passed explicitly.
    """
    app_id = app_id or os.getenv("OXR_APP_ID")
    if not app_id:
        raise RuntimeError("Missing Open Exchange Rates app id. Set OXR_APP_ID.")
    url = f"https://openexchangerates.org/api/latest.json?app_id={app_id}&symbols=GBP"
    s = _requests_session_with_retries()
    r = s.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    try:
        rate = float(data["rates"]["GBP"])
    except Exception as e:
        log.error("Unexpected OXR payload: %s", data)
        raise KeyError("Open Exchange Rates response missing rates.GBP") from e
    if rate <= 0:
        raise ValueError(f"Invalid GBP rate: {rate}")
    return rate

def _transform_frame(
    df: pl.DataFrame,
    usd_to_gbp: float,
    price_threshold_gbp: float = 100.0,
) -> pl.DataFrame:
    missing = REQUIRED_COLS - set(df.columns)
    # If 'name' is missing but 'title' exists, use 'title' as 'name'
    if "name" in missing and "title" in df.columns:
        df = df.with_columns([
            pl.col("title").alias("name")
        ])
        missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {sorted(missing)}")

    return (
        df.with_columns([
            pl.col("id").cast(pl.Int64, strict=False),
            pl.col("price").cast(pl.Float64, strict=False),
            (pl.col("price") * pl.lit(usd_to_gbp)).alias("price_gbp"),
            pl.col("category").cast(pl.Utf8, strict=False).str.to_titlecase().alias("category"),
            pl.lit("USD").alias("currency"),
            pl.lit(usd_to_gbp).alias("rate_to_gbp"),
            (pl.col("price") * pl.lit(usd_to_gbp) > pl.lit(price_threshold_gbp)).alias("is_premium"),
            (pl.col("price") * pl.lit(usd_to_gbp) > pl.lit(price_threshold_gbp)).alias("price_flag"),
        ])
        .with_columns([
            pl.when(pl.col("price_flag")).then(pl.lit("high")).otherwise(pl.lit("normal")).alias("price_flag")
        ])
        .select(
            "id",
            "name",
            "category",
            "currency",
            "price",
            "rate_to_gbp",
            "price_gbp",
            "is_premium",
            "price_flag",
            *[c for c in df.columns if c not in {"id", "name", "category", "currency", "price", "rate_to_gbp", "price_gbp", "is_premium", "price_flag"}],
        )
    )

def transform_products(
    raw_path: str | os.PathLike = RAW_DEFAULT,
    out_path: str | os.PathLike = OUT_DEFAULT,
    price_threshold: float = 100.0,
    usd_to_gbp: Optional[float] = None,
) -> str:
    """
    Read raw parquet (USD prices), convert to GBP, engineer columns, write cleaned parquet.
    """
    raw_path = str(raw_path)
    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if usd_to_gbp is None:
        usd_to_gbp = get_usd_to_gbp_rate()

    ext = Path(raw_path).suffix.lower()
    log.info("Reading raw data from %s", raw_path)
    if ext in {".json", ".ndjson"}:
        df_in = pl.read_ndjson(raw_path)
    elif ext == ".parquet":
        df_in = pl.read_parquet(raw_path)
    else:
        raise ValueError(f"Unsupported input file format: {ext}")

    log.info("Transforming %d rows", df_in.height)
    df_out = _transform_frame(df_in, usd_to_gbp=usd_to_gbp, price_threshold_gbp=price_threshold)

    log.info("Writing cleaned parquet to %s (%d rows)", out_path, df_out.height)
    df_out.write_parquet(out_path)

    return out_path

if __name__ == "__main__":
    transform_products()
