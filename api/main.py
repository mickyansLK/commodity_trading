from fastapi import FastAPI
import duckdb
import polars as pl

app = FastAPI()
DB_PATH = "../db/products.duckdb"

@app.get("/products")
def get_products():
    con = duckdb.connect(DB_PATH)
    df = pl.read_sql("SELECT * FROM products", con)
    con.close()
    return df.to_dicts()

@app.get("/products/{product_id}")
def get_product(product_id: int):
    con = duckdb.connect(DB_PATH)
    df = pl.read_sql(f"SELECT * FROM products WHERE id = {product_id}", con)
    con.close()
    if df.height == 0:
        return {"error": "Product not found"}
    return df.to_dicts()[0]
