from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from django.db import transaction

from ..models import Category, Order, OrderItem, Product, Seller, Store
from .idempotency_service import IdempotencyService


@dataclass(frozen=True)
class OrderIngestionResult:
    order: Order | None
    created: bool
    duplicated: bool
    idempotency_reason: str


class OrderIngestionService:
    """
    Serviço responsável por processar um payload de pedido com idempotência.

    Payload esperado (exemplo):

    {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "source": "LIVE",
        "external_id": "ORDER-1001",
        "customer_id": "CUSTOMER-1",
        "store": {
            "external_id": "STORE-1",
            "name": "Loja Centro",
            "region": "CE"
        },
        "seller": {
            "external_id": "SELLER-1",
            "name": "Maria"
        },
        "channel": "ONLINE",
        "status": "PAID",
        "ordered_at": "2026-03-08T10:00:00Z",
        "ingested_at": "2026-03-08T10:00:05Z",
        "canceled_at": null,
        "returned_at": null,
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
                    "category": {
                        "name": "Periféricos"
                    }
                },
                "quantity": 1,
                "unit_price": "100.00",
                "gross_amount": "100.00",
                "discount_amount": "10.00",
                "net_amount": "90.00",
                "cost_amount": "40.00"
            }
        ]
    }
    """

    @classmethod
    def process(cls, payload):
        source = str(payload.get("source") or "LIVE")
        external_id = str(payload["external_id"])
        event_id = payload.get("event_id")

        idempotency_key = IdempotencyService.build_key(
            source=source.lower(),
            resource_type="order",
            resource_id=external_id,
            event_id=str(event_id) if event_id else None,
        )

        payload_hash = IdempotencyService.build_payload_hash(payload)

        acquire_result = IdempotencyService.acquire(
            key=idempotency_key,
            source=source,
            event_id=event_id,
            resource_type="order",
            resource_id=external_id,
            payload_hash=payload_hash,
            allow_retry_failed=True,
        )

        if not acquire_result.should_process:
            order = Order.objects.filter(external_id=external_id).first()
            return OrderIngestionResult(
                order=order,
                created=False,
                duplicated=True,
                idempotency_reason=acquire_result.reason,
            )

        try:
            with transaction.atomic():
                store = cls._upsert_store(payload["store"])
                seller = cls._upsert_seller(payload["seller"], store=store)

                order, created = cls._upsert_order(
                    payload=payload,
                    store=store,
                    seller=seller,
                    event_id=event_id,
                    source=source,
                )

                cls._replace_items(order=order, items_payload=payload.get("items", []))

            IdempotencyService.mark_processed(acquire_result.record, response_code=200)

            return OrderIngestionResult(
                order=order,
                created=created,
                duplicated=False,
                idempotency_reason=acquire_result.reason,
            )
        except Exception as exc:
            IdempotencyService.mark_failed(
                acquire_result.record,
                error_message=str(exc),
                response_code=500,
            )
            raise

    @classmethod
    def _upsert_store(cls, payload):
        external_id = str(payload["external_id"])
        defaults = {
            "name": payload["name"],
            "region": payload["region"],
            "is_active": payload.get("is_active", True),
        }
        store, _ = Store.objects.update_or_create(
            external_id=external_id,
            defaults=defaults,
        )
        return store

    @classmethod
    def _upsert_seller(cls, payload, *, store):
        external_id = str(payload["external_id"])
        defaults = {
            "name": payload["name"],
            "store": store,
            "is_active": payload.get("is_active", True),
        }
        seller, _ = Seller.objects.update_or_create(
            external_id=external_id,
            defaults=defaults,
        )
        return seller

    @classmethod
    def _upsert_category(cls, payload):
        name = str(payload["name"]).strip()
        category, _ = Category.objects.get_or_create(name=name)
        return category

    @classmethod
    def _upsert_product(cls, payload):
        category_payload = payload["category"]
        category = cls._upsert_category(category_payload)

        external_id = str(payload["external_id"])
        defaults = {
            "name": payload["name"],
            "category": category,
            "is_active": payload.get("is_active", True),
        }
        product, _ = Product.objects.update_or_create(
            external_id=external_id,
            defaults=defaults,
        )
        return product

    @classmethod
    def _upsert_order(
        cls, *, payload, store, seller, event_id, source,
    ):
        external_id = str(payload["external_id"])

        defaults = {
            "event_id": event_id,
            "customer_id": payload.get("customer_id", ""),
            "store": store,
            "seller": seller,
            "channel": payload["channel"],
            "status": payload.get("status"),
            "source": source,
            "ordered_at": payload["ordered_at"],
            "ingested_at": payload["ingested_at"],
            "canceled_at": payload.get("canceled_at"),
            "returned_at": payload.get("returned_at"),
            "gross_amount": cls._to_decimal(payload.get("gross_amount")),
            "discount_amount": cls._to_decimal(payload.get("discount_amount")),
            "shipping_amount": cls._to_decimal(payload.get("shipping_amount", "0.00")),
            "net_revenue": cls._to_decimal(payload.get("net_revenue")),
            "cogs": cls._to_decimal(payload.get("cogs")),
        }

        order, created = Order.objects.update_or_create(
            external_id=external_id,
            defaults=defaults,
        )
        return order, created

    @classmethod
    def _replace_items(cls, *, order, items_payload):
        order.items.all().delete()

        items_to_create: list[OrderItem] = []

        for item_payload in items_payload:
            product = cls._upsert_product(item_payload["product"])

            items_to_create.append(
                OrderItem(
                    order=order,
                    product=product,
                    quantity=int(item_payload["quantity"]),
                    unit_price=cls._to_decimal(item_payload.get("unit_price")),
                    gross_amount=cls._to_decimal(item_payload.get("gross_amount")),
                    discount_amount=cls._to_decimal(item_payload.get("discount_amount", "0.00")),
                    net_amount=cls._to_decimal(item_payload.get("net_amount")),
                    cost_amount=cls._to_decimal(item_payload.get("cost_amount")),
                )
            )

        if items_to_create:
            OrderItem.objects.bulk_create(items_to_create)

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        if value in (None, ""):
            return Decimal("0.00")

        return Decimal(str(value))
