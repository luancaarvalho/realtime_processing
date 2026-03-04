from __future__ import annotations

import time

from confluent_kafka.admin import AdminClient, NewTopic

from settings import SETTINGS


def wait_for_kafka(timeout_seconds: int = 120, min_brokers: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap_servers})
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=3)
            if len(metadata.brokers) >= min_brokers:
                return
        except Exception:
            pass
        time.sleep(2)

    raise TimeoutError(f"Kafka did not become available with at least {min_brokers} brokers")


def ensure_topic(topic_name: str, num_partitions: int = 6, replication_factor: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic_name in metadata.topics:
        return

    broker_count = max(1, len(metadata.brokers))
    effective_rf = min(replication_factor, broker_count)

    futures = admin.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=effective_rf,
            )
        ]
    )
    for _, future in futures.items():
        future.result(15)
