#bibliotecas
from __future__ import annotations
import random
from datetime import datetime
from typing import Dict

#dimensões
SELLERS = ["S1", "S2", "S3", "S4", "S5"]
STORES = ["Store_A", "Store_B", "Store_C"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Sports"]
CHANNELS = ["Online", "Mobile", "InStore"]

#função para gerar os parâmetros
def build_order_event(
    rng: random.Random,
    order_id: str,
    event_time: datetime,
) -> Dict[str, object]:

#escolher os parâmetros aleatoriamente
    seller_id = rng.choice(SELLERS)
    store_id = rng.choice(STORES)
    category = rng.choice(CATEGORIES)
    channel = rng.choice(CHANNELS)

    quantity = rng.randint(1, 5)
    unit_price = round(rng.uniform(50, 2000), 2)

    gross_value = round(quantity * unit_price, 2)

    discount_rate = rng.uniform(0.05, 0.40)  # 5% a 40%
    discount_value = round(gross_value * discount_rate, 2)

    net_revenue = round(gross_value - discount_value, 2)

    cost_rate = rng.uniform(0.40, 0.75)
    cost = round(gross_value * cost_rate, 2)

    gross_profit = round(net_revenue - cost, 2)

    is_cancelled = rng.random() < 0.05
    is_returned = rng.random() < 0.08

#retornar base de dados
    return {
        "order_id": order_id,
        "event_time": event_time.isoformat(),
        "seller_id": seller_id,
        "store_id": store_id,
        "category": category,
        "channel": channel,
        "quantity": quantity,
        "unit_price": unit_price,
        "gross_value": gross_value,
        "discount_value": discount_value,
        "net_revenue": net_revenue,
        "cost": cost,
        "gross_profit": gross_profit,
        "is_cancelled": is_cancelled,
        "is_returned": is_returned,
    }