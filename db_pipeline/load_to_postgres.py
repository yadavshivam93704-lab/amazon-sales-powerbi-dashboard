import pandas as pd
import psycopg2
from psycopg2 import sql
import os

# ================================
# CONFIG
# ================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CSV_PATH = os.path.join(
    BASE_DIR,
    "master",
    "amazon_india_master_2015_2025.csv"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Amazon",
    "user": "postgres",
    "password": "Shivam@220"
}

# ================================
# LOAD CSV
# ================================

print("📂 Loading CSV from:", CSV_PATH)

df = pd.read_csv(CSV_PATH)

print("Rows:", df.shape[0])
print("Columns:", df.shape[1])

# ================================
# CONNECT TO POSTGRES
# ================================

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

print("✅ Connected to PostgreSQL")

# ================================
# CREATE STAGING TABLE
# ================================

cur.execute("""
DROP TABLE IF EXISTS staging_raw;

CREATE TABLE staging_raw (
    transaction_id TEXT,
    order_date DATE,

    customer_id TEXT,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,

    original_price_inr NUMERIC,
    discount_percent NUMERIC,
    discounted_price_inr NUMERIC,

    quantity INT,
    subtotal_inr NUMERIC,
    delivery_charges NUMERIC,
    final_amount_inr NUMERIC,

    customer_city TEXT,
    customer_state TEXT,
    customer_tier TEXT,
    customer_spending_tier TEXT,
    customer_age_group TEXT,

    payment_method TEXT,

    delivery_days INT,
    delivery_type TEXT,

    is_prime_member BOOLEAN,
    is_festival_sale BOOLEAN,
    festival_name TEXT,

    customer_rating NUMERIC,
    return_status TEXT,

    order_month INT,
    order_year INT,
    order_quarter INT,

    product_weight_kg NUMERIC,
    is_prime_eligible BOOLEAN,
    product_rating NUMERIC
);
""")

print("📦 staging_raw table created")

# ================================
# LOAD DATA USING COPY
# ================================

with open(CSV_PATH, "r", encoding="utf-8") as f:
    cur.copy_expert(
        """
        COPY staging_raw
        FROM STDIN
        WITH CSV HEADER
        DELIMITER ','
        """,
        f
    )

print("🚀 Data loaded into staging_raw")

# ================================
# CLOSE CONNECTION
# ================================

cur.close()
conn.close()

print("🎯 LOAD COMPLETED SUCCESSFULLY")
