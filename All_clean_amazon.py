import pandas as pd
import numpy as np
import re

# ======================================================
# CONFIGURATION
# ======================================================

YEAR_FILES = [
    "amazon_india_2015.csv",
    "amazon_india_2016.csv",
    "amazon_india_2017.csv",
    "amazon_india_2018.csv",
    "amazon_india_2019.csv",
    "amazon_india_2020.csv",
    "amazon_india_2021.csv",
    "amazon_india_2022.csv",
    "amazon_india_2023.csv",
    "amazon_india_2024.csv",
    "amazon_india_2025.csv"
]

CATALOGUE_FILE = "amazon_india_products_catalog.csv"

# ======================================================
# LOAD PRODUCT CATALOGUE (USED FOR ALL YEARS)
# ======================================================

catalogue = pd.read_csv(CATALOGUE_FILE)
catalogue = catalogue[["product_id", "base_price_2015"]]

print("✅ Product catalogue loaded")

# ======================================================
# LOOP THROUGH EACH YEAR FILE
# ======================================================

for FILE_NAME in YEAR_FILES:

    print("\n==============================================")
    print(f"Processing file: {FILE_NAME}")
    print("==============================================")

    df = pd.read_csv(FILE_NAME)
    print("Initial shape:", df.shape)

    # ======================================================
    # QUESTION 1: Clean & Standardize order_date → YYYY-MM-DD
    # ======================================================
    def parse_date(date):
        try:
            return pd.to_datetime(date, dayfirst=True, errors="coerce").date()
        except:
            return np.nan

    df["order_date"] = df["order_date"].apply(parse_date)

    # ======================================================
    # QUESTION 2: Clean original_price_inr
    # ======================================================
    def clean_price(val):
        if pd.isna(val):
            return np.nan

        val = str(val).strip().lower()

        if "price" in val:
            return np.nan

        val = (
            val.replace("₹", "")
               .replace("rs", "")
               .replace("â‚¹", "")
               .replace(",", "")
               .strip()
        )

        try:
            price = float(val)
            return abs(price)
        except:
            return np.nan

    df["original_price_inr"] = df["original_price_inr"].apply(clean_price)
    df["original_price_inr"].fillna(df["original_price_inr"].median(), inplace=True)

    # ======================================================
    # QUESTION 3: Clean customer_rating (1.0–5.0)
    # ======================================================
    def clean_rating(val):
        if pd.isna(val):
            return np.nan

        val = str(val).strip().lower()

        if re.match(r"^\d(\.\d)?$", val):
            return float(val)

        if "star" in val:
            return float(re.findall(r"\d(\.\d)?", val)[0])

        if "/" in val:
            num, denom = val.split("/")
            return round((float(num) / float(denom)) * 5, 1)

        return np.nan

    df["customer_rating"] = df["customer_rating"].apply(clean_rating)

    # ======================================================
    # QUESTION 4: Standardize customer_city
    # ======================================================
    city_map = {
     # Bangalore
        "bangalore": "Bengaluru",
        "bengaluru": "Bengaluru",
        "banglore": "Bengaluru",
        "bangaluru": "Bengaluru",
        "Bengalore": "Bengaluru",
        "Bangalore": "Bengaluru",
        "Banglore": "Bengaluru",
        "Bengalore": "Bengaluru",
        "Bengaluru": "Bengaluru",
        # Mumbai
        "mumbai": "Mumbai",
        "bombay": "Mumbai",
        "mumba": "Mumbai",
        "Bombay": "Mumbai",

        # Delhi
        "delhi": "Delhi",
        "new delhi": "Delhi",
        "delhi ncr": "Delhi",
        "Delhi NCR": "Delhi",
        "New Delhi": "Delhi",

        # Chennai
        "chennai": "Chennai",
        "chenai": "Chennai",
        "madras": "Chennai",
        "Madras": "Chennai",

        # Kolkata
        "kolkata": "Kolkata",
        "calcutta": "Kolkata",

        # Hyderabad
        "hyderabad": "Hyderabad",

        # Pune
        "pune": "Pune",

        # Ahmedabad
        "ahmedabad": "Ahmedabad",

        # Jaipur
        "jaipur": "Jaipur",

        # Lucknow
        "lucknow": "Lucknow",

        # Indore
        "indore": "Indore",

        # Kochi
        "kochi": "Kochi",
        "cochin": "Kochi",

        # Coimbatore
        "coimbatore": "Coimbatore",

        # Bhubaneswar
        "bhubaneswar": "Bhubaneswar",

        # Chandigarh
        "chandigarh": "Chandigarh",

        # Vadodara
        "vadodara": "Vadodara",

        # Surat
        "surat": "Surat",

        # Nagpur
        "nagpur": "Nagpur",

        # Meerut
        "meerut": "Meerut",

        # Moradabad
        "moradabad": "Moradabad",

        # Saharanpur
        "saharanpur": "Saharanpur",

        # Gorakhpur
        "gorakhpur": "Gorakhpur",

        # Kanpur
        "kanpur": "Kanpur",

        # Bareilly
        "bareilly": "Bareilly",

        # Aligarh
        "aligarh": "Aligarh",

        # Allahabad (Prayagraj old name kept)
        "allahabad": "Allahabad",

        # Visakhapatnam
        "visakhapatnam": "Visakhapatnam",

        # Patna
        "patna": "Patna",

        # Ludhiana
        "ludhiana": "Ludhiana",

        # Varanasi
        "varanasi": "Varanasi"
    }

    def clean_city(city):
        if pd.isna(city):
            return np.nan
        city = str(city).strip().lower()
        return city_map.get(city, city.title())

    df["customer_city"] = df["customer_city"].apply(clean_city)

    # ======================================================
    # QUESTION 5: Boolean Columns → True / False
    # ======================================================
    bool_cols = ["is_prime_member", "is_prime_eligible", "is_festival_sale"]

    def clean_boolean(val):
        if pd.isna(val):
            return False
        return str(val).lower().strip() in ["true", "yes", "1", "y"]

    for col in bool_cols:
        df[col] = df[col].apply(clean_boolean)

    # ======================================================
    # QUESTION 6: Standardize category
    # ======================================================
    def clean_category(val):
        if pd.isna(val):
            return "Unknown"
        val = str(val).lower().replace("&", "and")
        if "electronic" in val:
            return "Electronics"
        return val.title()

    df["category"] = df["category"].apply(clean_category)

    # ======================================================
    # QUESTION 7: Clean delivery_days
    # ======================================================
    def clean_delivery_days(val):
        if pd.isna(val):
            return np.nan

        val = str(val).lower()

        if "same" in val:
            return 0
        if "express" in val:
            return 1
        if "-" in val:
            nums = re.findall(r"\d+", val)
            return int(max(nums)) if nums else np.nan

        try:
            num = int(float(val))
            return min(max(num, 1), 30)
        except:
            return np.nan

    df["delivery_days"] = df["delivery_days"].apply(clean_delivery_days)
    df["delivery_days"].fillna(int(df["delivery_days"].median()), inplace=True)

    # ======================================================
    # QUESTION 8: Handle duplicates
    # ======================================================
    dup_cols = ["customer_id", "product_id", "order_date", "final_amount_inr"]
    df["is_duplicate"] = df.duplicated(subset=dup_cols, keep=False)
    df = df[~((df["is_duplicate"]) & (df["quantity"] == 1))]
    df.drop(columns=["is_duplicate"], inplace=True)

    # ======================================================
    # QUESTION 9: Correct Outlier Prices using Catalogue
    # ======================================================
    df = df.merge(catalogue, on="product_id", how="left")

    def correct_price(row):
        sp, bp = row["original_price_inr"], row["base_price_2015"]
        if pd.isna(sp) or pd.isna(bp):
            return sp
        if sp >= bp * 100:
            return round(sp / 100, 2)
        if sp >= bp * 10:
            return round(sp / 10, 2)
        if sp > bp * 3 or sp < bp * 0.3:
            return round(bp, 2)
        return round(sp, 2)

    df["original_price_inr"] = df.apply(correct_price, axis=1)
    df.drop(columns=["base_price_2015"], inplace=True)

    # ======================================================
    # QUESTION 10: Clean payment_method
    # ======================================================
    payment_map = {
        "upi": "UPI", "gpay": "UPI", "phonepe": "UPI", "paytm": "UPI",
        "credit": "Credit Card", "cc": "Credit Card",
        "debit": "Debit Card",
        "net banking": "Net Banking",
        "wallet": "Wallet",
        "bnpl": "BNPL",
        "cod": "Cash on Delivery"
    }

    def clean_payment(val):
        if pd.isna(val):
            return "Unknown"
        val = str(val).lower()
        for k, v in payment_map.items():
            if k in val:
                return v
        return "Other"

    df["payment_method"] = df["payment_method"].apply(clean_payment)

    # ======================================================
    # SAVE CLEANED FILE
    # ======================================================
    year = FILE_NAME.split("_")[-1].replace(".csv", "")
    output_file = f"amazon_india_{year}_cleaned.csv"
    df.to_csv(output_file, index=False)

    print("Final shape:", df.shape)
    print("✅ Saved:", output_file)
