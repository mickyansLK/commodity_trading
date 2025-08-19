"""ETL fetch step: Download product data from API and save as Parquet or JSON."""
import os
import logging
import requests
import polars as pl

OUTPUT_PATH = "data/raw/products.parquet"

def fetch_products(api_url: str = "https://fakestoreapi.com/products", out_path: str = OUTPUT_PATH, output_format: str = "parquet") -> None:
    """Fetch products from API and save to file. Modular for Airflow task usage."""
    if api_url.startswith("http"):
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        products = response.json()
    else:
        with open(api_url, "r", encoding="utf-8") as f:
            products = json.load(f)
    if not isinstance(products, list):
        raise ValueError("Source is not a list of products")
    df = pl.DataFrame(products)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if output_format == "json":
        df.write_ndjson(out_path.replace('.parquet', '.json'))
    elif output_format == "parquet":
        df.write_parquet(out_path)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    logging.info("Fetched and saved raw products to %s", out_path)
import json

def main():
    """Main entry point for script execution."""
    try:
        fetch_products()
    except Exception as exc:
        logging.error("Error fetching products: %s", exc)



if __name__ == "__main__":
    main()