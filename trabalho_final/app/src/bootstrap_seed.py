from __future__ import annotations

import time
from pathlib import Path

import orjson
import polars as pl
from confluent_kafka import Producer

from kafka_utils import wait_for_kafka
from minio_utils import build_s3_client, ensure_bucket, upload_bytes
from settings import SETTINGS


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")


def _publish_seed(df: pl.DataFrame) -> None:
    producer = Producer({"bootstrap.servers": SETTINGS.kafka_bootstrap_servers, "acks": "all"})

    sent = 0
    for row in df.iter_rows(named=True):
        payload = orjson.dumps(row)
        producer.produce(
            SETTINGS.kafka_topic_orders,
            key=str(row["order_id"]),
            value=payload,
            callback=_delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 2000 == 0:
            print(f"Seed publish progress: {sent}/{df.height}")
            time.sleep(0.2)

    producer.flush(30)
    print(f"Seed published to Kafka: {sent} records")


def _upload_seed_files() -> None:
    client = build_s3_client()
    ensure_bucket(client)

    for path in [SETTINGS.seed_orders_path, SETTINGS.seed_targets_path]:
        file_path = Path(path)
        key = f"seed/{file_path.name}"
        payload = file_path.read_bytes()
        upload_bytes(client, key, payload, "text/csv")
        print(f"Uploaded seed to MinIO: s3://{SETTINGS.minio_bucket}/{key}")


def main() -> None:
    wait_for_kafka()

    orders_path = Path(SETTINGS.seed_orders_path)
    if not orders_path.exists():
        raise FileNotFoundError(f"Seed file not found: {orders_path}")

    df = pl.read_csv(orders_path)
    _publish_seed(df)
    _upload_seed_files()


if __name__ == "__main__":
    main()
