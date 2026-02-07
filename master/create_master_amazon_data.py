import pandas as pd
import os

# ==========================================
# MASTER DATA CREATION (2015–2025)
# ==========================================

# Folder structure:
# Amazon/
# ├── amazon_india_2015_cleaned.csv
# ├── amazon_india_2016_cleaned.csv
# ├── ...
# ├── amazon_india_2025_cleaned.csv
# └── master/
#     └── create_master_amazon_data.py

BASE_PATH = ".."  # go one level up from /master

YEAR_FILES = [
    "amazon_india_2015_cleaned.csv",
    "amazon_india_2016_cleaned.csv",
    "amazon_india_2017_cleaned.csv",
    "amazon_india_2018_cleaned.csv",
    "amazon_india_2019_cleaned.csv",
    "amazon_india_2020_cleaned.csv",
    "amazon_india_2021_cleaned.csv",
    "amazon_india_2022_cleaned.csv",
    "amazon_india_2023_cleaned.csv",
    "amazon_india_2024_cleaned.csv",
    "amazon_india_2025_cleaned.csv"
]

all_dfs = []

print("📥 Loading cleaned yearly files...")

for file in YEAR_FILES:
    file_path = os.path.join(BASE_PATH, file)

    if not os.path.exists(file_path):
        print(f"❌ File missing: {file_path}")
        continue

    df = pd.read_csv(file_path)
    print(f"✅ Loaded {file} | Shape: {df.shape}")
    all_dfs.append(df)

# ==========================================
# COMBINE ALL YEARS
# ==========================================
master_df = pd.concat(all_dfs, ignore_index=True)

print("\n📊 MASTER DATA SUMMARY")
print("Total Records:", master_df.shape[0])
print("Total Columns:", master_df.shape[1])
print(
    "Year Range:",
    master_df["order_year"].min(),
    "-",
    master_df["order_year"].max()
)

# ==========================================
# SAVE MASTER FILE
# ==========================================
OUTPUT_FILE = "amazon_india_master_2015_2025.csv"
master_df.to_csv(OUTPUT_FILE, index=False)

print("\n✅ MASTER DATASET CREATED SUCCESSFULLY")
print(f"📁 File saved as: {OUTPUT_FILE}")
