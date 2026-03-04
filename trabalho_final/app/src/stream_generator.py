from __future__ import annotations

import random
import time
from datetime import UTC, datetime
from pathlib import Path

import orjson
import polars as pl
from confluent_kafka import Producer

from catalog_data import default_targets
from event_factory import build_order_event
from kafka_utils import wait_for_kafka
from settings import SETTINGS


def load_target_map() -> dict[str, dict[str, float | int]]:
    targets_path = Path(SETTINGS.seed_targets_path)
    if targets_path.exists():
        df = pl.read_csv(targets_path)
        return {
            row["seller_id"]: {
                "target_gmv": float(row["target_gmv"]),
                "target_orders": int(row["target_orders"]),
            }
            for row in df.iter_rows(named=True)
        }

    return {
        row["seller_id"]: {
            "target_gmv": float(row["target_gmv"]),
            "target_orders": int(row["target_orders"]),
        }
        for row in default_targets()
    }


def main() -> None:
    wait_for_kafka()

    rng = random.Random(2026)
    target_map = load_target_map()

    producer = Producer({"bootstrap.servers": SETTINGS.kafka_bootstrap_servers, "acks": "all"})
    eps = max(1, SETTINGS.generator_events_per_second)
    sleep_interval = 1.0 / eps

    sequence = 1
    print(f"Live generator started at {eps} events/s")

    while True:
        event = build_order_event(
            rng=rng,
            order_id=f"RT-{sequence:09d}",
            event_time=datetime.now(UTC),
            target_by_seller=target_map,
        )
        sequence += 1

        producer.produce(
            SETTINGS.kafka_topic_orders,
            key=str(event["order_id"]),
            value=orjson.dumps(event),
        )
        producer.poll(0)

        if sequence % (eps * 3) == 0:
            producer.flush(2)
            print(f"Live records sent: {sequence - 1}")

        time.sleep(sleep_interval)


if __name__ == "__main__":
    main()
