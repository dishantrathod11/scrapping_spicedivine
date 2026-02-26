import requests
import pandas as pd
import re
import time
from datetime import datetime
import psycopg2

# =========================
# CONFIG
# =========================

BASE_URL = "https://spicedivine.ca"
PRODUCTS_API = BASE_URL + "/collections/all/products.json"
SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

#databse config
DB_CONFIG = {
    "host": "127.0.0.1",
    "database": "canada_stores",
    "user": "dishant",
    "password": "admin123",
    "port": "5432"
}

#Create Table Function

def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS spicedivine_products (
            id SERIAL PRIMARY KEY,
            store TEXT,
            keyword TEXT,
            product_name TEXT,
            unit_size TEXT,
            price_text TEXT,
            price NUMERIC,
            url TEXT,
            scrape_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (url, unit_size, scrape_date)
        );
    """)

#Insert Function

def insert_into_postgres(df):

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_table(cursor)

    insert_count = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO spicedivine_products
            (store, keyword, product_name, unit_size, price_text, price, url, scrape_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (url, unit_size, scrape_date) DO NOTHING
            RETURNING id;
        """,
        (
            row["Store"],
            row["Keyword"],
            row["Product Name"],
            row["Unit Size"],
            row["Price Text"],
            row["Price"],
            row["URL"],
            row["Scrape Date"]
        ))

        if cursor.fetchone():
            insert_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ {insert_count} NEW records inserted into spicedivine_products table")



# =========================
# KEYWORDS
# =========================

keywords = ["Ajwain seed", "Amchur Powder", "Anardana Powder", "Anardana Whole", "Tukmaria", "Bay Leaves", "Black Pepper Powder", 
            "Black Pepper Whole", "Black Salt", "Cardamom Black", "Cardamom Powder", "Cardamom Whole", "Citric Acid", "Chilli Powder Kashmiri", 
            "Chilli Powder", "Chilli Powder Extra Hot", "Chilli Whole Kashmiri", "Chilli Whole", "Chilli Round", "Chilli Crushed", "Cinnamon", "Cinnamon Powder", 
            "Cinnamon Sticks Round", "Clove Powder", "Clove Whole", "Coriander Powder", "Coriander seed", "Cumin Powder", "Cumin seed", "Coriander Cumin Powder", 
            "Curry Powder", "Dhana Dal", "Fennel LAKHNAVI", "Fennel Powder", "Fennel seed", "Fenugreek Powder", "Fenugreek seed", "Flax seed", "Garam Masala Powder", 
            "Garam Masala Whole", "Garlic Powder", "Ginger Powder", "Javantri Powder", "Javantri Whole", "Shah Jeera", "Kalonji", "Mustard seed", "Jaiphal Whole", "Jaiphal Powder", 
            "Panchpuran", "Paprika", "Poppy seed", "Sesame seed Black", "Sesame seed", "Sesame seed White", "Turmeric Powder", "Turmeric Whole", 
            "White Pepper Powder", "White Pepper Whole", "Mint Leaves", "Kasoori Methi", "Black Beans", "Black Eye Beans", "Brown Chori", "Chana Dal", 
            "Rajma Chitra", "Desi Val", "Vatana Green", "Vatana white", "Horse Gram", "Kabuli chana", "Desi chana", "Rajma Kashmiri", "Rajma Red", "Masoor Dal", 
            "Masoor Whole", "Moong Dal", "Moong Dal Yellow", "Moong Whole Desi", "Moong Whole", "Moth", "Red Chori", "Toor Dal", "Toor Whole", "Urad Dal", "Urad Whole", 
            "Chana Mosambi", "Khichdi Mix", "Basmati Rice Rozana", "Brown Sona Masoori", "Diabetic Rice", "Long Grain Basmati Rice", "Sella Parboiled Basmati Rice", 
            "Sona Masoori Rice", "Classic Basmati Rice", "Golden Sella", "Ponni Boiled Rice", "Basmati Long Premium Rice", "Basmati Super Rice", "Sabudana", "Bajri Flour", 
            "Bajri Mamra", "Besan Flour", "Bhakhri Flour", "Whole Wheat Atta", "Whole Wheat Premium Atta", "Chilli Flakes", "Chilli Powder Resham Patti", "Chora", 
            "Coconut Shredded Thin", "Corn Fryums", "Corn Poha", "Dhokla Flour", "Fada", "Handva Flour", "Jaggery Cube", "Jaggery Powder", "Jaggery Slab", 
            "Jowar Flour", "Juwar Mamra", "Ladu Besan", "Methi Kuriya", "Mini Spiral Fryums", "Mini Wavy Fryums", "Mustard seed Small", "Pickle Masala", "Pickle Masala Golkry", 
            "Poha Nylon", "Poha Thick", "Ragi Mamra", "Ragi Panipuri Fryums", "Rice Flour", "Sakar", "Sweet Makhana", "Sooji", "Soyabean", "Stone Flower", "Sugar", "Wheat Puffs", 
            "Long Basmati Rice", "Cardamom seed", "Oregano", "Harde", "Jamun Powder", "Corn Starch", "Baking Powder", "Char Goond Powder", "Khmeer", "Chandan Powder", 
            "Mulethi Powder", "Guggul", "Triphala Whole", "Charoli"]

# =========================
# HELPERS
# =========================

def extract_unit_and_clean_name(product_name):
    # Regex to capture number + unit
    pattern = r'(\d+(?:\.\d+)?\s?(g|kg|ml|lb|lbs|l|oz|pcs|pc|pack))'
    match = re.search(pattern, product_name, re.IGNORECASE)

    if match:
        unit = match.group(1).strip()

        # Clean up unit string
        unit = unit.replace("#", "")           # remove stray #
        unit = unit.replace(":", "")           # remove colon if any
        unit = unit.replace("lbs", "lb")       # normalize lb
        unit = re.sub(r"\s+", " ", unit)      # remove multiple spaces

        # Normalize casing
        unit = re.sub(r"g\b", "g", unit, flags=re.IGNORECASE)
        #
        unit = re.sub(r"oz\b", "oz", unit, flags=re.IGNORECASE)
        unit = re.sub(r"pc\b", "pc", unit, flags=re.IGNORECASE)
        unit = re.sub(r"pcs\b", "pcs", unit, flags=re.IGNORECASE)
        unit = re.sub(r"pack\b", "pack", unit, flags=re.IGNORECASE)

        clean_name = product_name.replace(match.group(1), "").strip()
    else:
        unit = ""
        clean_name = product_name.strip()

    # Remove extra characters from clean_name if necessary
    clean_name = re.sub(r"[\#:]", "", clean_name).strip()

    return clean_name, unit

def keyword_match(product_name, keyword):
    """
    Returns True if ALL words from keyword exist in product_name (case-insensitive)
    """
    product_name = product_name.lower()
    keyword_words = keyword.lower().split()
    return all(word in product_name for word in keyword_words)

# =========================
# SCRAPER
# =========================

products_data = []
visited_products = set()  # To avoid duplicate product URLs

def scrape_spicedivine():

    page = 1

    while True:
        print(f"\n📄 Scraping page {page}")
        params = {"page": page, "limit": 250}
        response = requests.get(PRODUCTS_API, headers=headers, params=params)

        if response.status_code != 200:
            print("❌ Failed to fetch page")
            break

        data = response.json()
        products = data.get("products", [])

        if not products:
            print("✅ No more products found.")
            break

        print(f"Products found: {len(products)}")

        for product in products:

            handle = product.get("handle", "")
            base_title = product.get("title", "")
            product_url = f"{BASE_URL}/products/{handle}"

            if product_url in visited_products:
                continue
            visited_products.add(product_url)

            variants = product.get("variants", [])

            for variant in variants:

                variant_title = variant.get("title", "")
                raw_price = variant.get("price", "0")

                # Convert price to float
                try:
                    price = float(str(raw_price).replace(",", ""))
                except:
                    price = None

                # Build full name
                full_name = base_title
                if variant_title and variant_title.lower() != "default title":
                    full_name = f"{base_title} - {variant_title}"

                # Determine unit_size
                # Determine unit_size safely
                if variant_title.lower() != "default title":
                    unit_size = variant_title.strip()

                    # Basic cleanup only
                    unit_size = unit_size.replace("#", "").strip()
                    unit_size = re.sub(r"\s+", "", unit_size)  # remove spaces (500 ml → 500ml)

                else:
                    # Only if no real variant exists, try extracting from product name
                    clean_name, unit_size = extract_unit_and_clean_name(base_title)

                    unit_size = unit_size.replace("#", "").strip()
                    unit_size = re.sub(r"\s+", "", unit_size)

                # Clean product name (remove unit from name)
                clean_name, _ = extract_unit_and_clean_name(full_name)

                # Match first keyword
                # Check against each keyword like Swadesh
                matched_keyword = None

                for keyword in keywords:
                    if keyword_match(clean_name, keyword):
                        matched_keyword = keyword
                        break

                # If no keyword matched, skip product
                if not matched_keyword:
                    continue

                products_data.append({
                    "Store": "Spice Divine",
                    "Keyword": matched_keyword,
                    "Product Name": clean_name,
                    "Unit Size": unit_size,
                    "Price Text": f"${price}" if price else "N/A",
                    "Price": price,
                    "URL": product_url,
                    "Scrape Date": SCRAPE_DATE
                })

                print(f"✔ {clean_name} | Keyword: {matched_keyword} | {unit_size} | ${price}")

        page += 1
        time.sleep(1)

# =========================
# MAIN
# =========================

def main():

    scrape_spicedivine()

    if products_data:

        df = pd.DataFrame(products_data)

        print(f"\n📊 Before Cleaning: {len(df)} rows")

        #df = df[df["Keyword"] != "Uncategorized"]
        df = df[df["Unit Size"].notna()]
        df = df[df["Unit Size"].str.strip() != ""]

        print(f"📊 After Cleaning: {len(df)} rows")

        filename = f"spicedivine_products_{SCRAPE_DATE}.csv"
        df.to_csv(filename, index=False)

        print(f"\n✅ CSV Saved: {filename}")
        print(f"📊 Final Products Saved: {len(df)}")

        # 🔥 INSERT INTO DATABASE
        insert_into_postgres(df)

    else:
        print("❌ No products scraped")


if __name__ == "__main__":
    main()