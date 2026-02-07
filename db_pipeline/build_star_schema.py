import psycopg2
import os

# ================================
# DB CONFIG
# ================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Amazon",
    "user": "postgres",
    "password": "Shivam@220"  
}

# ================================
# SQL SCRIPT
# ================================

SQL_SCRIPT = """
SET search_path TO public;

DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS time_dimension CASCADE;

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    product_weight_kg NUMERIC,
    is_prime_eligible BOOLEAN,
    product_rating NUMERIC(3,2)
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_city TEXT,
    customer_state TEXT,
    customer_tier TEXT,
    customer_spending_tier TEXT,
    customer_age_group TEXT,
    is_prime_member BOOLEAN
);

CREATE TABLE time_dimension (
    date_key DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    month_name TEXT,
    week INT,
    day INT,
    day_name TEXT,
    is_weekend BOOLEAN
);

CREATE TABLE transactions (

    transaction_id TEXT PRIMARY KEY,

    order_date DATE,
    date_key DATE REFERENCES time_dimension(date_key),

    customer_id TEXT REFERENCES customers(customer_id),
    product_id TEXT REFERENCES products(product_id),

    quantity INT,
    subtotal_inr NUMERIC,
    discount_percent NUMERIC,
    discounted_price_inr NUMERIC,
    delivery_charges NUMERIC,
    final_amount_inr NUMERIC,

    delivery_days INT,
    delivery_type TEXT,

    payment_method TEXT,
    return_status TEXT,

    is_festival_sale BOOLEAN,
    festival_name TEXT,

    customer_rating NUMERIC(3,2)
);

-- LOAD DIMENSIONS

INSERT INTO products
SELECT DISTINCT
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    product_weight_kg,
    is_prime_eligible,
    product_rating
FROM staging_raw;

INSERT INTO customers
SELECT
    customer_id,
    MAX(customer_city),
    MAX(customer_state),
    MAX(customer_tier),
    MAX(customer_spending_tier),
    MAX(customer_age_group),
    BOOL_OR(is_prime_member)
FROM staging_raw
GROUP BY customer_id;

INSERT INTO time_dimension
SELECT
    order_date,
    MAX(order_year),
    MAX(order_quarter),
    MAX(order_month),
    TO_CHAR(order_date,'Month'),
    EXTRACT(WEEK FROM order_date),
    EXTRACT(DAY FROM order_date),
    TO_CHAR(order_date,'Day'),
    CASE 
        WHEN EXTRACT(DOW FROM order_date) IN (0,6)
        THEN TRUE ELSE FALSE 
    END
FROM staging_raw
GROUP BY order_date;

-- LOAD FACT

INSERT INTO transactions
SELECT
    transaction_id,
    order_date,
    order_date,
    customer_id,
    product_id,
    quantity,
    subtotal_inr,
    discount_percent,
    discounted_price_inr,
    delivery_charges,
    final_amount_inr,
    delivery_days,
    delivery_type,
    payment_method,
    return_status,
    is_festival_sale,
    festival_name,
    customer_rating
FROM staging_raw;

-- INDEXES

CREATE INDEX idx_txn_date ON transactions(order_date);
CREATE INDEX idx_txn_customer ON transactions(customer_id);
CREATE INDEX idx_txn_product ON transactions(product_id);
CREATE INDEX idx_txn_return ON transactions(return_status);
CREATE INDEX idx_txn_amount ON transactions(final_amount_inr);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_subcategory ON products(subcategory);

CREATE INDEX idx_customers_city ON customers(customer_city);
CREATE INDEX idx_customers_state ON customers(customer_state);

-- VALIDATION

SELECT COUNT(*) FROM staging_raw;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM time_dimension;
SELECT COUNT(*) FROM transactions;
"""

# ================================
# EXECUTION
# ================================

print("🚀 Connecting to PostgreSQL...")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

print("✅ Connected")

cur.execute(SQL_SCRIPT)

print("🎯 Star schema built successfully")

cur.close()
conn.close()
