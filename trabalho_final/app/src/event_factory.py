from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta

from catalog_data import CAMPAIGNS, CHANNELS, PAYMENT_METHODS, PRODUCTS, SELLERS


def _iso_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _weighted_hour(rng: random.Random) -> int:
    weights = [1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 6, 7, 8, 9, 9, 10, 11, 11, 10, 9, 6, 4, 3, 2]
    return rng.choices(list(range(24)), weights=weights, k=1)[0]


def random_black_friday_timestamp(rng: random.Random, reference_date: datetime) -> datetime:
    day_start = reference_date.replace(hour=0, minute=0, second=0, microsecond=0)
    hour = _weighted_hour(rng)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return day_start + timedelta(hours=hour, minutes=minute, seconds=second)


def build_order_event(
    rng: random.Random,
    order_id: str,
    event_time: datetime,
    target_by_seller: dict[str, dict[str, float | int]],
) -> dict[str, str | int | float]:
    seller = rng.choice(SELLERS)
    product = rng.choice(PRODUCTS)
    channel = rng.choices(CHANNELS, weights=[0.42, 0.30, 0.20, 0.08], k=1)[0]

    qty = rng.choices([1, 2, 3, 4, 5], weights=[0.56, 0.27, 0.10, 0.05, 0.02], k=1)[0]
    unit_price = round(rng.uniform(product["unit_price_min"], product["unit_price_max"]), 2)
    gross_amount = round(unit_price * qty, 2)

    peak_bonus = 0.09 if 15 <= event_time.hour <= 20 else 0.03
    base_discount = float(product["base_discount"]) + peak_bonus
    discount_pct = min(0.75, max(0.05, rng.normalvariate(base_discount, 0.07)))
    discount_amount = round(gross_amount * discount_pct, 2)

    shipping_amount = 0.0 if channel == "loja_fisica" else round(rng.uniform(8, 39), 2)
    cogs_rate = min(0.90, max(0.20, float(product["cogs_rate"]) + rng.uniform(-0.06, 0.06)))
    cogs = round(gross_amount * cogs_rate, 2)

    p_cancel = 0.04 + (0.05 if discount_pct >= 0.55 else 0.0)
    p_return = 0.05 + (0.03 if product["category"] in {"Moda", "Moveis"} else 0.0)
    u = rng.random()
    if u < p_cancel:
        status = "CANCELLED"
    elif u < p_cancel + p_return:
        status = "RETURNED"
    else:
        status = "PAID"

    net_revenue = round(gross_amount - discount_amount + shipping_amount, 2)

    target_info = target_by_seller.get(seller["seller_id"], {})

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": _iso_utc(event_time),
        "ingestion_time": _iso_utc(datetime.now(UTC)),
        "order_id": order_id,
        "order_date": event_time.date().isoformat(),
        "customer_id": f"C{rng.randint(1, 9000):05d}",
        "seller_id": seller["seller_id"],
        "seller_name": seller["seller_name"],
        "store_id": seller["store_id"],
        "store_name": seller["store_name"],
        "region": seller["region"],
        "channel": channel,
        "category": product["category"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "quantity": qty,
        "unit_price": unit_price,
        "gross_amount": gross_amount,
        "discount_pct": round(discount_pct, 4),
        "discount_amount": discount_amount,
        "shipping_amount": shipping_amount,
        "net_revenue": net_revenue,
        "cogs": cogs,
        "status": status,
        "payment_method": rng.choice(PAYMENT_METHODS),
        "campaign": rng.choice(CAMPAIGNS),
        "target_gmv": float(target_info.get("target_gmv", 320000.0)),
        "target_orders": int(target_info.get("target_orders", 450)),
    }
