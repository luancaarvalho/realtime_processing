import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

ORDERS_PATH = Path("dados_raw/orders_seed.csv")
SELLERS_PATH = Path("dados_raw/sellers_targets.csv")

TOTAL_ORDERS = 20_000
TOTAL_SELLERS = 10

REFERENCE_DATE = datetime(2025, 11, 28, tzinfo=timezone.utc)

# ==============================
# STATIC DATA
# ==============================

REGIONS = ["N", "NE", "CO", "SE", "S"]
CHANNELS = ["app", "web"]
CATEGORIES = ["Moda", "Eletronicos", "Casa", "Beleza"]
PAYMENT_METHODS = ["credito", "pix", "boleto", "carteira_digital"]
STATUS_OPTIONS = ["PAID", "CANCELLED"]
CAMPAIGNS = ["flash_deals", "black_week", "none"]

PRODUCTS = [
    ("P001", "Smartphone X"),
    ("P002", "Notebook Pro"),
    ("P003", "Jaqueta Premium"),
    ("P004", "Air Fryer"),
    ("P005", "Perfume Luxo"),
]

# ==============================
# SELLERS GENERATION
# ==============================

def generate_sellers():
    sellers = []
    for i in range(1, TOTAL_SELLERS + 1):
        sellers.append({
            "seller_id": f"S{i:03d}",
            "seller_name": f"Seller {i}",
            "target_gmv": round(random.uniform(200000, 600000), 2),
            "target_orders": random.randint(300, 800),
            "target_date": "2025-11-28"
        })
    return sellers


# ==============================
# RANDOM TIMESTAMP
# ==============================

def random_black_friday_timestamp():
    seconds_offset = random.randint(0, 24 * 60 * 60)
    return REFERENCE_DATE + timedelta(seconds=seconds_offset)


# ==============================
# ORDER GENERATION
# ==============================

def generate_orders(sellers):
    with ORDERS_PATH.open("w", newline="", encoding="utf-8") as fp:
        writer = None

        for i in range(TOTAL_ORDERS):
            seller = random.choice(sellers)
            product_id, product_name = random.choice(PRODUCTS)

            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(50, 1500), 2)
            gross_amount = round(quantity * unit_price, 2)

            discount_pct = round(random.uniform(0, 0.30), 3)
            discount_amount = round(gross_amount * discount_pct, 2)

            shipping_amount = round(random.uniform(10, 50), 2)

            net_revenue = round(gross_amount - discount_amount + shipping_amount, 2)
            cogs = round(net_revenue * random.uniform(0.4, 0.7), 2)

            event_time = random_black_friday_timestamp()
            ingestion_time = datetime.now(timezone.utc)

            row = {
                "event_id": str(uuid.uuid4()),
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "ingestion_time": ingestion_time.isoformat().replace("+00:00", "Z"),
                "order_id": f"SEED-{i+1:07d}",
                "order_date": "2025-11-28",
                "customer_id": f"C{random.randint(10000,99999)}",
                "seller_id": seller["seller_id"],
                "seller_name": seller["seller_name"],
                "store_id": f"LJ{random.randint(1,20):02d}",
                "store_name": "Black Friday Store",
                "region": random.choice(REGIONS),
                "channel": random.choice(CHANNELS),
                "category": random.choice(CATEGORIES),
                "product_id": product_id,
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "gross_amount": gross_amount,
                "discount_pct": discount_pct,
                "discount_amount": discount_amount,
                "shipping_amount": shipping_amount,
                "net_revenue": net_revenue,
                "cogs": cogs,
                "status": random.choice(STATUS_OPTIONS),
                "payment_method": random.choice(PAYMENT_METHODS),
                "campaign": random.choice(CAMPAIGNS),
                "target_gmv": seller["target_gmv"],
                "target_orders": seller["target_orders"],
            }

            if writer is None:
                writer = csv.DictWriter(fp, fieldnames=row.keys())
                writer.writeheader()

            writer.writerow(row)

            if i % 10000 == 0 and i > 0:
                print(f"{i} pedidos gerados...")

    print("Orders CSV finalizado!")


# ==============================
# MAIN
# ==============================

def main():
    ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SELLERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    sellers = generate_sellers()

    with SELLERS_PATH.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=sellers[0].keys())
        writer.writeheader()
        writer.writerows(sellers)

    print("Sellers CSV gerado!")

    generate_orders(sellers)

    print("Dataset completo gerado com sucesso!")
    
    print("\nPreview do dataset:\n")
    df_preview = pd.read_csv(ORDERS_PATH, nrows=5)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df_preview)


if __name__ == "__main__":
    main()