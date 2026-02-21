from __future__ import annotations

import argparse
import json
import time
from typing import Any

import requests
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC = "pinot_upsert_orders"
SCHEMA_NAME = "pinot_upsert_orders"
TABLE_NAME = "pinot_upsert_orders"
KAFKA_BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
PINOT_CONTROLLER = "http://localhost:19000"


SCHEMA: dict[str, Any] = {
    "schemaName": SCHEMA_NAME,
    "dimensionFieldSpecs": [
        {"name": "order_id", "dataType": "INT"},
        {"name": "customer_id", "dataType": "STRING"},
        {"name": "status", "dataType": "STRING"},
    ],
    "metricFieldSpecs": [{"name": "amount", "dataType": "DOUBLE"}],
    "dateTimeFieldSpecs": [
        {
            "name": "event_time",
            "dataType": "LONG",
            "format": "1:MILLISECONDS:EPOCH",
            "granularity": "1:MILLISECONDS",
        }
    ],
    "primaryKeyColumns": ["order_id"],
}


TABLE_CONFIG: dict[str, Any] = {
    "tableName": TABLE_NAME,
    "tableType": "REALTIME",
    "segmentsConfig": {
        "timeColumnName": "event_time",
        "timeType": "MILLISECONDS",
        "schemaName": SCHEMA_NAME,
        "replication": "1",
    },
    "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
    "tableIndexConfig": {"loadMode": "MMAP"},
    "routing": {"instanceSelectorType": "strictReplicaGroup"},
    "upsertConfig": {"mode": "FULL"},
    "ingestionConfig": {
        "streamIngestionConfig": {
            "streamConfigMaps": [
                {
                    "streamType": "kafka",
                    "stream.kafka.topic.name": TOPIC,
                    "stream.kafka.broker.list": "kafka:29092",
                    "stream.kafka.consumer.type": "lowLevel",
                    "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
                    "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
                    "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
                    "realtime.segment.flush.threshold.rows": "100000",
                    "realtime.segment.flush.threshold.time": "1h",
                }
            ]
        }
    },
}


def wait_http(url: str, timeout_s: int = 120) -> None:
    start = time.time()
    while True:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code < 500:
                return
        except Exception:
            pass

        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timeout aguardando endpoint: {url}")
        time.sleep(2)


def ensure_topic(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if TOPIC in metadata.topics:
        print(f"Kafka topic ja existe: {TOPIC}")
        return

    futures = admin.create_topics(
        [
            NewTopic(
                TOPIC,
                num_partitions=1,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        ]
    )
    futures[TOPIC].result(timeout=20)
    print(f"Kafka topic criado: {TOPIC}")


def delete_if_exists(path: str) -> None:
    resp = requests.delete(f"{PINOT_CONTROLLER}{path}", timeout=10)
    if resp.status_code in (200, 202, 404):
        return
    print(f"Aviso ao deletar {path}: {resp.status_code} {resp.text}")


def post_json(path: str, payload: dict[str, Any]) -> None:
    resp = requests.post(
        f"{PINOT_CONTROLLER}{path}",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    if resp.status_code not in (200, 201, 202):
        raise RuntimeError(f"Erro POST {path}: {resp.status_code} {resp.text}")


def create_table_with_retry(retries: int = 20, sleep_s: int = 3) -> None:
    for attempt in range(1, retries + 1):
        resp = requests.post(
            f"{PINOT_CONTROLLER}/tables",
            data=json.dumps(TABLE_CONFIG),
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        if resp.status_code in (200, 201, 202):
            return

        can_retry = resp.status_code == 409 and "External view" in resp.text
        if can_retry and attempt < retries:
            print("Aguardando limpeza da external view da tabela...")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"Erro POST /tables: {resp.status_code} {resp.text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Configura topic + schema/table do demo Pinot Upsert")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    args = parser.parse_args()

    print("Aguardando Pinot Controller...")
    wait_http(f"{PINOT_CONTROLLER}/health")

    ensure_topic(args.bootstrap)

    # Recria schema/tabela para a demo ser reexecutavel.
    delete_if_exists(f"/tables/{TABLE_NAME}")
    delete_if_exists(f"/schemas/{SCHEMA_NAME}")

    post_json("/schemas", SCHEMA)
    create_table_with_retry()

    print("Pinot schema e tabela de upsert configurados.")


if __name__ == "__main__":
    main()
