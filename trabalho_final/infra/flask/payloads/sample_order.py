from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def build_sample_order_payload() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": str(uuid4()),
        "source": "LIVE",
        "external_id": f"ORDER-{uuid4().hex[:8].upper()}",
        "customer_id": "CUSTOMER-1",
        "store": {
            "external_id": "STORE-1",
            "name": "Loja Centro",
            "region": "CE",
            "is_active": True,
        },
        "seller": {
            "external_id": "SELLER-1",
            "name": "Maria",
            "is_active": True,
        },
        "channel": "ONLINE",
        "status": "PAID",
        "ordered_at": now,
        "ingested_at": now,
        "canceled_at": None,
        "returned_at": None,
        "gross_amount": "100.00",
        "discount_amount": "10.00",
        "shipping_amount": "5.00",
        "net_revenue": "95.00",
        "cogs": "40.00",
        "items": [
            {
                "product": {
                    "external_id": "PROD-1",
                    "name": "Teclado",
                    "is_active": True,
                    "category": {
                        "name": "Periféricos",
                    },
                },
                "quantity": 1,
                "unit_price": "100.00",
                "gross_amount": "100.00",
                "discount_amount": "10.00",
                "net_amount": "90.00",
                "cost_amount": "40.00",
            }
        ],
    }
