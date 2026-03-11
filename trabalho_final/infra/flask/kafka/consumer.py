from __future__ import annotations

import json, time

from confluent_kafka import Consumer, KafkaError

from config.settings import Config
from services.django_client import DjangoIngestClient


class OrderConsumer:
    def __init__(self) -> None:
        self.topic = Config.KAFKA_ORDER_TOPIC
        self.client = DjangoIngestClient()
        self.consumer = Consumer(
            {
                "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "flask-order-consumer-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def run(self) -> None:
        self.consumer.subscribe([self.topic])
        print(f"[CONSUMER] Escutando topic={self.topic}")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    error = msg.error()

                    if error.code() == KafkaError._PARTITION_EOF:
                        continue

                    print(f"[CONSUMER] Erro no Kafka: {error}")
                    time.sleep(2)
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as exc:
                    print(f"[CONSUMER] Payload inválido: {exc}")
                    self.consumer.commit(message=msg)
                    continue

                print(
                    f"[CONSUMER] Mensagem recebida "
                    f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
                )

                try:
                    response = self.client.send_order(payload)
                    print(f"[CONSUMER] Django respondeu: {response}")

                    self.consumer.commit(message=msg)
                    print("[CONSUMER] Offset confirmado com sucesso")

                except Exception as exc:
                    print(f"[CONSUMER] Erro ao enviar para Django: {exc}")
                    time.sleep(2)
        finally:
            self.consumer.close()
