from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.db.models import Avg, Count, Sum, Value, DecimalField, Q
from django.db.models.functions import Coalesce
from django.utils import timezone

from apps.sales.models import Order


ZERO_DECIMAL = Decimal("0.00")


def _parse_window(window: str) -> timedelta:
    mapping = {
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "6h": timedelta(hours=6),
        "12h": timedelta(hours=12),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
    }
    return mapping.get(window, timedelta(minutes=15))


def _get_orders_queryset(window: str):
    start = timezone.now() - _parse_window(window)
    return Order.objects.filter(created_at__gte=start)


def _to_float(value) -> float:
    return float(value or 0)


def get_dashboard_kpis(window: str = "15m") -> dict:
    qs = _get_orders_queryset(window)

    aggregates = qs.aggregate(
        gmv=Coalesce(
            Sum("gross_amount"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        net_profit=Coalesce(
            Sum("net_revenue"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        avg_ticket=Coalesce(
            Avg("gross_amount"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        orders_count=Count("id"),
        avg_discount=Coalesce(
            Avg("discount_amount"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        cost=Coalesce(
            Sum("cogs"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        discount_impact=Coalesce(
            Sum("discount_amount"),
            Value(ZERO_DECIMAL),
            output_field=DecimalField(max_digits=18, decimal_places=2),
        ),
        cancelations=Count("id", filter=Q(canceled_at__isnull=False)),
        returns=Count("id", filter=Q(returned_at__isnull=False)),
    )

    gross_margin = (aggregates["net_profit"] or ZERO_DECIMAL) - (aggregates["cost"] or ZERO_DECIMAL)

    target_orders = 500
    target_vs_actual = (
        (aggregates["orders_count"] / target_orders) * 100 if target_orders else 0
    )

    return {
        "c_level": {
            "gmv": _to_float(aggregates["gmv"]),
            "net_profit": _to_float(aggregates["net_profit"]),
            "avg_ticket": _to_float(aggregates["avg_ticket"]),
        },
        "sales": {
            "orders_count": aggregates["orders_count"],
            "avg_discount": _to_float(aggregates["avg_discount"]),
            "target_vs_actual": round(target_vs_actual, 2),
        },
        "controllership": {
            "gross_margin": _to_float(gross_margin),
            "cost": _to_float(aggregates["cost"]),
            "discount_impact": _to_float(aggregates["discount_impact"]),
            "cancelations": aggregates["cancelations"],
            "returns": aggregates["returns"],
        },
        "window": window,
    }


def get_seller_rankings(limit: int = 5, window: str = "15m") -> list[dict]:
    qs = (
        _get_orders_queryset(window)
        .values("seller__name")
        .annotate(
            total_sales=Coalesce(
                Sum("gross_amount"),
                Value(ZERO_DECIMAL),
                output_field=DecimalField(max_digits=18, decimal_places=2),
            ),
            orders_count=Count("id"),
        )
        .order_by("-total_sales")[:limit]
    )

    return [
        {
            "label": row["seller__name"] or "Sem vendedor",
            "value": _to_float(row["total_sales"]),
            "meta": f"Pedidos: {row['orders_count']}",
        }
        for row in qs
    ]


def get_store_rankings(limit: int = 5, window: str = "15m") -> list[dict]:
    qs = (
        _get_orders_queryset(window)
        .values("store__name", "store__region")
        .annotate(
            total_sales=Coalesce(
                Sum("gross_amount"),
                Value(ZERO_DECIMAL),
                output_field=DecimalField(max_digits=18, decimal_places=2),
            ),
            orders_count=Count("id"),
        )
        .order_by("-total_sales")[:limit]
    )

    return [
        {
            "label": row["store__name"] or "Sem loja",
            "value": _to_float(row["total_sales"]),
            "meta": row["store__region"] or f"Pedidos: {row['orders_count']}",
        }
        for row in qs
    ]


def get_category_profits(window: str = "15m") -> list[dict]:
    return []


def get_channel_profits(window: str = "15m") -> list[dict]:
    qs = (
        _get_orders_queryset(window)
        .values("channel")
        .annotate(
            total_revenue=Coalesce(
                Sum("net_revenue"),
                Value(ZERO_DECIMAL),
                output_field=DecimalField(max_digits=18, decimal_places=2),
            ),
            orders_count=Count("id"),
        )
        .order_by("-total_revenue")
    )

    return [
        {
            "label": row["channel"] or "Sem canal",
            "value": _to_float(row["total_revenue"]),
            "meta": f"Pedidos: {row['orders_count']}",
        }
        for row in qs
    ]
