from __future__ import annotations

SELLERS = [
    {"seller_id": "S001", "seller_name": "Ana Ferreira", "store_id": "LJ01", "store_name": "Centro Fortaleza", "region": "NE"},
    {"seller_id": "S002", "seller_name": "Bruno Costa", "store_id": "LJ02", "store_name": "Aldeota Premium", "region": "NE"},
    {"seller_id": "S003", "seller_name": "Carla Nunes", "store_id": "LJ03", "store_name": "North Shopping", "region": "NE"},
    {"seller_id": "S004", "seller_name": "Diego Ramos", "store_id": "LJ04", "store_name": "Iguatemi Express", "region": "NE"},
    {"seller_id": "S005", "seller_name": "Elaine Lima", "store_id": "LJ05", "store_name": "E-commerce BR", "region": "BR"},
    {"seller_id": "S006", "seller_name": "Felipe Duarte", "store_id": "LJ06", "store_name": "Marketplace Prime", "region": "BR"},
    {"seller_id": "S007", "seller_name": "Gabriela Sousa", "store_id": "LJ07", "store_name": "Sul Hub", "region": "S"},
    {"seller_id": "S008", "seller_name": "Henrique Alves", "store_id": "LJ08", "store_name": "Sudeste Hub", "region": "SE"},
    {"seller_id": "S009", "seller_name": "Isabela Rocha", "store_id": "LJ09", "store_name": "Norte Hub", "region": "N"},
    {"seller_id": "S010", "seller_name": "Joao Martins", "store_id": "LJ10", "store_name": "Centro-Oeste Hub", "region": "CO"},
]

PRODUCTS = [
    {"product_id": "P001", "product_name": "Smart TV 55", "category": "Eletronicos", "unit_price_min": 1800, "unit_price_max": 3200, "base_discount": 0.20, "cogs_rate": 0.62},
    {"product_id": "P002", "product_name": "Notebook Gamer", "category": "Eletronicos", "unit_price_min": 4200, "unit_price_max": 7800, "base_discount": 0.23, "cogs_rate": 0.66},
    {"product_id": "P003", "product_name": "Fone Bluetooth", "category": "Eletronicos", "unit_price_min": 180, "unit_price_max": 420, "base_discount": 0.30, "cogs_rate": 0.45},
    {"product_id": "P004", "product_name": "Geladeira Inverter", "category": "Eletrodomesticos", "unit_price_min": 2500, "unit_price_max": 4900, "base_discount": 0.18, "cogs_rate": 0.63},
    {"product_id": "P005", "product_name": "Air Fryer XL", "category": "Eletrodomesticos", "unit_price_min": 320, "unit_price_max": 760, "base_discount": 0.28, "cogs_rate": 0.48},
    {"product_id": "P006", "product_name": "Sofa 3 Lugares", "category": "Moveis", "unit_price_min": 1700, "unit_price_max": 3500, "base_discount": 0.22, "cogs_rate": 0.58},
    {"product_id": "P007", "product_name": "Mesa Jantar 6 Lugares", "category": "Moveis", "unit_price_min": 1400, "unit_price_max": 2900, "base_discount": 0.21, "cogs_rate": 0.55},
    {"product_id": "P008", "product_name": "Tenis Running", "category": "Moda", "unit_price_min": 190, "unit_price_max": 480, "base_discount": 0.35, "cogs_rate": 0.42},
    {"product_id": "P009", "product_name": "Jaqueta Premium", "category": "Moda", "unit_price_min": 260, "unit_price_max": 690, "base_discount": 0.34, "cogs_rate": 0.43},
    {"product_id": "P010", "product_name": "Perfume 100ml", "category": "Beleza", "unit_price_min": 180, "unit_price_max": 540, "base_discount": 0.32, "cogs_rate": 0.40},
    {"product_id": "P011", "product_name": "Kit Skincare", "category": "Beleza", "unit_price_min": 120, "unit_price_max": 310, "base_discount": 0.33, "cogs_rate": 0.38},
    {"product_id": "P012", "product_name": "Cadeira Gamer", "category": "Moveis", "unit_price_min": 680, "unit_price_max": 1550, "base_discount": 0.27, "cogs_rate": 0.50},
]

CHANNELS = ["site", "app", "marketplace", "loja_fisica"]
PAYMENT_METHODS = ["pix", "cartao_credito", "boleto", "carteira_digital"]
CAMPAIGNS = ["black_friday", "esquenta_black", "flash_deals", "queima_total"]


def default_targets() -> list[dict[str, str | float | int]]:
    targets = []
    for idx, seller in enumerate(SELLERS, start=1):
        targets.append(
            {
                "seller_id": seller["seller_id"],
                "seller_name": seller["seller_name"],
                "target_gmv": round(250_000 + idx * 32_500, 2),
                "target_orders": 350 + idx * 27,
                "target_date": "2025-11-28",
            }
        )
    return targets
