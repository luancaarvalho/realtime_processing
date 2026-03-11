from __future__ import annotations

import json
from confluent_kafka import Producer
from config.settings import Config


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[PRODUCER] Falha na entrega: {err}")
        return

    print(
        f"[PRODUCER] Mensagem entregue "
        f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
    )


class OrderProducer:
    def __init__(self) -> None:
        self.topic = Config.KAFKA_ORDER_TOPIC
        self.producer = Producer(
            {
                "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "flask-order-producer",
            }
        )

    def send(self, payload: dict) -> None:
        key = payload.get("external_id", "")
        value = json.dumps(payload).encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=str(key).encode("utf-8"),
            value=value,
            callback=delivery_report,
        )
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()
