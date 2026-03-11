from __future__ import annotations

import random, uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP


def money(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


STORES = [
    {"external_id": "STORE-001", "name": "Loja Centro"},
    {"external_id": "STORE-002", "name": "Loja Aldeota"},
    {"external_id": "STORE-003", "name": "Loja Sobral"},
]

SELLERS = [
    {"external_id": "SELLER-001", "name": "Ana Ribeiro", "store_external_id": "STORE-001"},
    {"external_id": "SELLER-002", "name": "Carlos Mendes", "store_external_id": "STORE-001"},
    {"external_id": "SELLER-003", "name": "Beatriz Lima", "store_external_id": "STORE-002"},
    {"external_id": "SELLER-004", "name": "João Matos", "store_external_id": "STORE-003"},
]

PRODUCTS = [
    {"external_id": "PROD-001", "name": "Notebook Pro", "category": "Informática"},
    {"external_id": "PROD-002", "name": "Mouse Gamer", "category": "Informática"},
    {"external_id": "PROD-003", "name": "Smartphone X", "category": "Smartphones"},
    {"external_id": "PROD-004", "name": "Fone Bluetooth", "category": "Eletrônicos"},
    {"external_id": "PROD-005", "name": "Carregador Turbo", "category": "Acessórios"},
]

CHANNELS = ["ECOMMERCE", "APP", "MARKETPLACE", "PHYSICAL_STORE"]
STATUS_WEIGHTS = [
    ("PAID", 90),
    ("CANCELED", 6),
    ("RETURNED", 4),
]


def weighted_choice(options):
    labels = [item[0] for item in options]
    weights = [item[1] for item in options]
    return random.choices(labels, weights=weights, k=1)[0]


class RandomOrderGenerator:
    @classmethod
    def generate_order(cls) -> dict:
        seller = random.choice(SELLERS)
        store = next(s for s in STORES if s["external_id"] == seller["store_external_id"])
        status = weighted_choice(STATUS_WEIGHTS)
        channel = random.choice(CHANNELS)

        items = []
        total_gross = Decimal("0.00")
        total_discount = Decimal("0.00")
        total_cost = Decimal("0.00")

        item_count = random.randint(1, 4)
        selected_products = random.sample(PRODUCTS, k=min(item_count, len(PRODUCTS)))

        for product in selected_products:
            quantity = random.randint(1, 3)
            unit_price = money(random.uniform(49.90, 3500.00))
            gross_amount = money(unit_price * quantity)

            discount_amount = money(random.uniform(0, float(gross_amount) * 0.18))
            net_amount = gross_amount - discount_amount

            cost_factor = Decimal(str(random.uniform(0.45, 0.82)))
            cost_amount = money(net_amount * cost_factor)

            total_gross += gross_amount
            total_discount += discount_amount
            total_cost += cost_amount

            items.append(
                {
                    "product_external_id": product["external_id"],
                    "quantity": quantity,
                    "unit_price": str(unit_price),
                    "gross_amount": str(gross_amount),
                    "discount_amount": str(discount_amount),
                    "net_amount": str(net_amount),
                    "cost_amount": str(cost_amount),
                }
            )

        shipping_amount = money(random.uniform(0, 40))
        net_revenue = total_gross - total_discount + shipping_amount

        now = datetime.now(timezone.utc).isoformat()
        canceled_at = now if status == "CANCELED" else None
        returned_at = now if status == "RETURNED" else None

        return {
            "external_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "event_id": str(uuid.uuid4()),
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "store_external_id": store["external_id"],
            "seller_external_id": seller["external_id"],
            "channel": channel,
            "status": status,
            "source": "SIMULATION",
            "ordered_at": now,
            "ingested_at": now,
            "canceled_at": canceled_at,
            "returned_at": returned_at,
            "gross_amount": str(money(total_gross)),
            "discount_amount": str(money(total_discount)),
            "shipping_amount": str(money(shipping_amount)),
            "net_revenue": str(money(net_revenue)),
            "cogs": str(money(total_cost)),
            "items": items,
        }
