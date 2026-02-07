"""
Validate Warehouse Row Counts
-----------------------------
Checks staging vs star-schema tables.

Run:
python db_pipeline/validate_warehouse_counts.py
"""

import psycopg2

# ================================
# DB CONFIG
# ================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Amazon",
    "user": "postgres",
    "password": "Shivam@220"   # <-- CHANGE
}

# ================================
# QUERIES TO RUN
# ================================

QUERIES = {
    "staging_raw": "SELECT COUNT(*) FROM staging_raw;",
    "products": "SELECT COUNT(*) FROM products;",
    "customers": "SELECT COUNT(*) FROM customers;",
    "time_dimension": "SELECT COUNT(*) FROM time_dimension;",
    "transactions": "SELECT COUNT(*) FROM transactions;",
}

# ================================
# EXECUTION
# ================================

print("🚀 Connecting to PostgreSQL...")

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

print("✅ Connected\n")

print("📊 WAREHOUSE VALIDATION COUNTS")
print("-" * 40)

for table, query in QUERIES.items():
    cur.execute(query)
    count = cur.fetchone()[0]
    print(f"{table:<15} : {count}")

cur.close()
conn.close()

print("\n🎯 Validation completed successfully.")
