-- SQLite schema for product pricing data
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    title TEXT,
    category TEXT,
    price_usd REAL,
    price_gbp REAL,
    price_flag TEXT
);
