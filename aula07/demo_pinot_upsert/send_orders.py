from __future__ import annotations

import argparse
import json
from dataclasses import dataclass

from confluent_kafka import Producer

TOPIC = "pinot_upsert_orders"
BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"


@dataclass(frozen=True)
class OrderEvent:
    order_id: int
    customer_id: str
    status: str
    amount: float
    event_time: int


# Carga inicial: 10 registros fixos
INITIAL_10 = [
    OrderEvent(1, "C001", "CREATED", 120.0, 1730000000001),
    OrderEvent(2, "C002", "CREATED", 90.0, 1730000000002),
    OrderEvent(3, "C003", "CREATED", 200.0, 1730000000003),
    OrderEvent(4, "C004", "CREATED", 75.0, 1730000000004),
    OrderEvent(5, "C005", "CREATED", 330.0, 1730000000005),
    OrderEvent(6, "C006", "CREATED", 50.0, 1730000000006),
    OrderEvent(7, "C007", "CREATED", 410.0, 1730000000007),
    OrderEvent(8, "C008", "CREATED", 130.0, 1730000000008),
    OrderEvent(9, "C009", "CREATED", 99.0, 1730000000009),
    OrderEvent(10, "C010", "CREATED", 280.0, 1730000000010),
]

# Upsert: 3 registros fixos
# - order_id=3: UPDATE (substitui valor anterior)
# - order_id=11 e 12: INSERTS (novas chaves)
UPSERT_3 = [
    OrderEvent(3, "C003", "PAID", 260.0, 1730000001011),
    OrderEvent(11, "C011", "CREATED", 145.0, 1730000001012),
    OrderEvent(12, "C012", "CREATED", 500.0, 1730000001013),
]


def as_dict(evt: OrderEvent) -> dict:
    return {
        "order_id": evt.order_id,
        "customer_id": evt.customer_id,
        "status": evt.status,
        "amount": evt.amount,
        "event_time": evt.event_time,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Envia eventos para demo de upsert no Pinot")
    parser.add_argument("--upsert", action="store_true", help="Envia os 3 eventos de upsert")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    events = UPSERT_3 if args.upsert else INITIAL_10
    stage = "UPSERT_3" if args.upsert else "INITIAL_10"

    print(f"Enviando lote {stage} para topic={args.topic}")
    for evt in events:
        print(as_dict(evt))

    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "1"})
    delivery_errors: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            delivery_errors.append(f"Erro key={msg.key()!r}: {err}")

    for evt in events:
        payload = json.dumps(as_dict(evt)).encode("utf-8")
        producer.produce(args.topic, key=str(evt.order_id), value=payload, callback=on_delivery)
        producer.poll(0)

    producer.flush(20)
    if delivery_errors:
        raise RuntimeError("Falha ao enviar eventos:\n" + "\n".join(delivery_errors))
    print(f"Lote enviado com sucesso: {len(events)} registros")


if __name__ == "__main__":
    main()
