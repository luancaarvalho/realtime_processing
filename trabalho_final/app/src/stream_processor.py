from __future__ import annotations

import time
from datetime import UTC, datetime

import orjson
import pyarrow as pa
from confluent_kafka import Consumer

from kafka_utils import ensure_topic, wait_for_kafka
from minio_utils import build_s3_client, ensure_bucket, upload_json, upload_parquet
from settings import SETTINGS


class MetricsState:
    def __init__(self) -> None:
        self.total_orders = 0
        self.gmv = 0.0
        self.net_revenue = 0.0
        self.discount_amount = 0.0
        self.cogs = 0.0
        self.cancelled = 0
        self.returned = 0

        self.by_seller: dict[str, dict[str, float | int | str]] = {}
        self.by_store: dict[str, dict[str, float | int | str]] = {}
        self.by_category: dict[str, dict[str, float]] = {}
        self.by_channel: dict[str, dict[str, float]] = {}

    def add_event(self, row: dict) -> None:
        status = str(row.get("status", "PAID"))

        gross_amount = float(row.get("gross_amount", 0.0))
        net_revenue = float(row.get("net_revenue", 0.0))
        discount_amount = float(row.get("discount_amount", 0.0))
        cogs = float(row.get("cogs", 0.0))

        if status == "CANCELLED":
            self.cancelled += 1
        elif status == "RETURNED":
            self.returned += 1

        self.total_orders += 1
        self.gmv += gross_amount
        self.net_revenue += net_revenue
        self.discount_amount += discount_amount
        self.cogs += cogs

        seller_id = str(row.get("seller_id", "UNKNOWN"))
        seller_name = str(row.get("seller_name", "Unknown"))
        target_gmv = float(row.get("target_gmv", 0.0))
        target_orders = int(row.get("target_orders", 0))

        seller = self.by_seller.setdefault(
            seller_id,
            {
                "seller_id": seller_id,
                "seller_name": seller_name,
                "orders": 0,
                "gmv": 0.0,
                "net_revenue": 0.0,
                "discount": 0.0,
                "target_gmv": target_gmv,
                "target_orders": target_orders,
            },
        )
        seller["orders"] = int(seller["orders"]) + 1
        seller["gmv"] = float(seller["gmv"]) + gross_amount
        seller["net_revenue"] = float(seller["net_revenue"]) + net_revenue
        seller["discount"] = float(seller["discount"]) + discount_amount

        store_id = str(row.get("store_id", "UNKNOWN"))
        store_name = str(row.get("store_name", "Unknown"))
        store = self.by_store.setdefault(
            store_id,
            {
                "store_id": store_id,
                "store_name": store_name,
                "orders": 0,
                "gmv": 0.0,
                "net_revenue": 0.0,
            },
        )
        store["orders"] = int(store["orders"]) + 1
        store["gmv"] = float(store["gmv"]) + gross_amount
        store["net_revenue"] = float(store["net_revenue"]) + net_revenue

        category = str(row.get("category", "Unknown"))
        category_item = self.by_category.setdefault(category, {"gmv": 0.0, "profit": 0.0})
        category_item["gmv"] = float(category_item["gmv"]) + gross_amount
        category_item["profit"] = float(category_item["profit"]) + (net_revenue - cogs)

        channel = str(row.get("channel", "Unknown"))
        channel_item = self.by_channel.setdefault(channel, {"gmv": 0.0, "profit": 0.0})
        channel_item["gmv"] = float(channel_item["gmv"]) + gross_amount
        channel_item["profit"] = float(channel_item["profit"]) + (net_revenue - cogs)

    def snapshot(self) -> dict:
        ticket_medio = self.gmv / self.total_orders if self.total_orders else 0.0
        desconto_medio = self.discount_amount / self.total_orders if self.total_orders else 0.0
        lucro_liquido = self.net_revenue - self.cogs
        margem_bruta = (lucro_liquido / self.net_revenue) if self.net_revenue else 0.0

        sellers = list(self.by_seller.values())
        sellers.sort(key=lambda x: float(x["gmv"]), reverse=True)

        stores = list(self.by_store.values())
        stores.sort(key=lambda x: float(x["gmv"]), reverse=True)

        meta_vs_realizado = []
        for s in sellers:
            target = float(s.get("target_gmv", 0.0))
            realizado = float(s.get("gmv", 0.0))
            pct = (realizado / target * 100.0) if target else 0.0
            meta_vs_realizado.append(
                {
                    "seller_id": s["seller_id"],
                    "seller_name": s["seller_name"],
                    "target_gmv": target,
                    "realizado_gmv": realizado,
                    "atingimento_pct": pct,
                }
            )

        categories = [{"category": k, **v} for k, v in self.by_category.items()]
        channels = [{"channel": k, **v} for k, v in self.by_channel.items()]
        categories.sort(key=lambda x: float(x["profit"]), reverse=True)
        channels.sort(key=lambda x: float(x["profit"]), reverse=True)

        return {
            "updated_at": datetime.now(UTC).isoformat(),
            "c_level": {
                "gmv": self.gmv,
                "lucro_liquido": lucro_liquido,
                "ticket_medio": ticket_medio,
            },
            "vendas": {
                "numero_pedidos": self.total_orders,
                "ticket_medio": ticket_medio,
                "desconto_medio": desconto_medio,
                "gmv_por_vendedor": sellers,
                "gmv_por_loja": stores,
                "top_performers": sellers[:5],
                "bottom_performers": list(reversed(sellers[-5:])),
                "meta_vs_realizado": meta_vs_realizado,
            },
            "controladoria": {
                "margem_bruta": margem_bruta,
                "custo": self.cogs,
                "impacto_descontos": self.discount_amount,
                "cancelamentos": self.cancelled,
                "devolucoes": self.returned,
                "lucro_por_categoria": categories,
                "lucro_por_canal": channels,
            },
        }


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": SETTINGS.kafka_bootstrap_servers,
            "group.id": "bf-processor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def upload_batch(client, rows: list[dict], batch_id: int) -> None:
    if not rows:
        return

    table = pa.Table.from_pylist(rows)
    now = datetime.now(UTC)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    key = f"processed/orders/year={now.year}/batch_{stamp}_{batch_id:06d}.parquet"
    upload_parquet(client, key, table)


def main() -> None:
    wait_for_kafka(timeout_seconds=180, min_brokers=1)
    ensure_topic(SETTINGS.kafka_topic_orders, num_partitions=3, replication_factor=1)

    s3 = build_s3_client()
    ensure_bucket(s3)

    consumer = build_consumer()
    consumer.subscribe([SETTINGS.kafka_topic_orders])

    state = MetricsState()
    buffer: list[dict] = []
    last_flush = time.time()
    batch_id = 1

    print("Processor started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.time()

            if msg is None:
                if buffer and (now - last_flush >= SETTINGS.processor_flush_seconds):
                    upload_batch(s3, buffer, batch_id)
                    upload_json(s3, SETTINGS.dashboard_metrics_key, state.snapshot())
                    print(f"Flushed idle batch with {len(buffer)} events")
                    buffer.clear()
                    batch_id += 1
                    last_flush = now
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            row = orjson.loads(msg.value())
            if not isinstance(row, dict):
                continue

            state.add_event(row)
            buffer.append(row)

            reached_batch = len(buffer) >= SETTINGS.processor_batch_size
            reached_time = (now - last_flush) >= SETTINGS.processor_flush_seconds

            if reached_batch or reached_time:
                upload_batch(s3, buffer, batch_id)
                upload_json(s3, SETTINGS.dashboard_metrics_key, state.snapshot())
                print(f"Flushed batch {batch_id} with {len(buffer)} events")
                buffer.clear()
                batch_id += 1
                last_flush = now

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
