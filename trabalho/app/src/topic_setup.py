from __future__ import annotations

from pathlib import Path

from generate_seed_dataset import main as generate_seed_dataset
from kafka_utils import ensure_topic, wait_for_kafka
from settings import SETTINGS


def main() -> None:
    wait_for_kafka(timeout_seconds=240, min_brokers=3)
    ensure_topic(SETTINGS.kafka_topic_orders, num_partitions=6, replication_factor=3)

    orders_path = Path(SETTINGS.seed_orders_path)
    targets_path = Path(SETTINGS.seed_targets_path)
    if not orders_path.exists() or not targets_path.exists():
        generate_seed_dataset()

    print(f"Topic ready: {SETTINGS.kafka_topic_orders}")
    print(f"Seed files ready: {orders_path} | {targets_path}")


if __name__ == "__main__":
    main()
