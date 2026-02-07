import pandas as pd
import numpy as np
import re

# ===============================
# LOAD DATA
# ===============================
FILE_NAME = "amazon_india_2015.csv"
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

    # Handle non-price text
    if "price" in val:
        return np.nan

    # Remove currency symbols & encoding junk
    val = (
        val.replace("₹", "")
           .replace("rs", "")
           .replace("â‚¹", "")
           .replace(",", "")
           .strip()
    )

    try:
        price = float(val)

        # Convert negative prices to positive
        if price < 0:
            price = abs(price)

        return price

    except:
        return np.nan

df["original_price_inr"] = df["original_price_inr"].apply(clean_price)

# Fill remaining NaN with median price (optional but recommended)
median_price = df["original_price_inr"].median()
df["original_price_inr"] = df["original_price_inr"].fillna(median_price)

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
def clean_city(city):
    if pd.isna(city):
        return np.nan

    city = str(city).strip().lower()

    city_map = {
        # Bangalore
        "bangalore": "Bengaluru",
        "bengaluru": "Bengaluru",
        "banglore": "Bengaluru",
        "bangaluru": "Bengaluru",
        "Bengalore": "Bengaluru",
        
        # Mumbai
        "mumbai": "Mumbai",
        "bombay": "Mumbai",
        "mumba": "Mumbai",

        # Delhi
        "delhi": "Delhi",
        "new delhi": "Delhi",
        "delhi ncr": "Delhi",

        # Chennai
        "chennai": "Chennai",
        "chenai": "Chennai",
        "madras": "Chennai",

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

    return city_map.get(city, city.title())

df["customer_city"] = df["customer_city"].apply(clean_city)

# ======================================================
# QUESTION 5: Boolean Columns → True / False
# ======================================================
bool_cols = ["is_prime_member", "is_prime_eligible", "is_festival_sale"]

def clean_boolean(val):
    if pd.isna(val):
        return False
    val = str(val).strip().lower()
    return val in ["true", "yes", "1", "y"]

for col in bool_cols:
    df[col] = df[col].apply(clean_boolean)

# ======================================================
# QUESTION 6: Standardize category
# ======================================================
def clean_category(val):
    if pd.isna(val):
        return "Unknown"

    val = str(val).lower().strip()

    # normalize symbols & spaces
    val = val.replace("&", "and")
    val = re.sub(r"\s+", " ", val)

    if "electronic" in val:
        return "Electronics"

    return val.title()

df["category"] = df["category"].apply(clean_category)
# ======================================================
# QUESTION 7: Clean delivery_days
# ======================================================
def clean_delivery_days(val):
    """
    Cleans delivery_days column and guarantees numeric output (no NaN)
    """
    if pd.isna(val):
        return np.nan

    val = str(val).strip().lower()

    # Text-based cases
    if "same" in val:
        return 0
    if "express" in val:
        return 1
    if "-" in val:  # e.g. "1-2 days"
        nums = re.findall(r"\d+", val)
        if nums:
            return int(max(nums))  # take worst-case
        else:
            return np.nan

    # Numeric cases
    try:
        num = int(float(val))
        if num < 0:
            return 1
        if num > 30:
            return 30
        return num
    except:
        return np.nan
df["delivery_days"] = df["delivery_days"].apply(clean_delivery_days)

# Fill any remaining NaN with median (no NaN requirement)
median_delivery = int(df["delivery_days"].median())
df["delivery_days"] = df["delivery_days"].fillna(median_delivery)

# ======================================================
# QUESTION 8: Handle duplicates
# ======================================================
dup_cols = ["customer_id", "product_id", "order_date", "final_amount_inr"]

df["is_duplicate"] = df.duplicated(subset=dup_cols, keep=False)

# Keep duplicates only if quantity > 1 (bulk order)
df = df[~((df["is_duplicate"]) & (df["quantity"] == 1))]

df.drop(columns=["is_duplicate"], inplace=True)

# ======================================================
# QUESTION 9: Correct Outlier Prices using Product Catalogue
# ======================================================

# Load product catalogue
catalogue = pd.read_csv("amazon_india_products_catalog.csv")

# Keep only required columns
catalogue = catalogue[["product_id", "base_price_2015"]]

# Merge catalogue price into main dataframe
df = df.merge(
    catalogue,
    on="product_id",
    how="left"
)

def correct_price(row):
    """
    Corrects price using catalogue reference
    Rules:
    1. If price is 10x or 100x of base price → divide accordingly
    2. If price is still unrealistic → replace with base_price_2015
    3. If base price missing → keep original
    """
    sale_price = row["original_price_inr"]
    base_price = row["base_price_2015"]

    if pd.isna(sale_price) or pd.isna(base_price):
        return sale_price

    # 100x decimal error
    if sale_price >= base_price * 100:
        return round(sale_price / 100, 2)

    # 10x decimal error
    if sale_price >= base_price * 10:
        return round(sale_price / 10, 2)

    # Still far from expected → replace with catalogue price
    if sale_price > base_price * 3 or sale_price < base_price * 0.3:
        return round(base_price, 2)

    # Otherwise keep original
    return round(sale_price, 2)

# Apply correction
df["original_price_inr"] = df.apply(correct_price, axis=1)

# Drop catalogue column after correction
df.drop(columns=["base_price_2015"], inplace=True)

print("✅ Question 9: Price outliers corrected using product catalogue")
# ======================================================
# QUESTION 10: CLEAN PAYMENT METHOD
# ======================================================

payment_map = {
    "upi": "UPI",
    "phonepe": "UPI",
    "google pay": "UPI",
    "gpay": "UPI",
    "paytm": "UPI",

    "credit": "Credit Card",
    "credit card": "Credit Card",
    "cc": "Credit Card",

    "debit": "Debit Card",
    "debit card": "Debit Card",

    "net banking": "Net Banking",
    "internet banking": "Net Banking",

    "wallet": "Wallet",

    "bnpl": "BNPL",

    "cod": "Cash on Delivery",
    "c.o.d": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery"
}

def clean_payment(val):
    if pd.isna(val):
        return "Unknown"

    val = str(val).lower().strip()

    for key, clean_val in payment_map.items():
        if key in val:
            return clean_val

    return "Other"

df["payment_method"] = df["payment_method"].apply(clean_payment)

print("Q10 Payment method cleaned")
# ===============================
# SAVE CLEANED DATA
# ===============================
OUTPUT_FILE = "amazon_india_2015_cleaned.csv"
df.to_csv(OUTPUT_FILE, index=False)

print("Final shape:", df.shape)
print("Cleaned file saved as:", OUTPUT_FILE)
