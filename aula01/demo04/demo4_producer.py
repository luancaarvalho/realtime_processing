import argparse
import random
import time
import uuid

import orjson
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topics(bootstrap, topics):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    md = admin.list_topics(timeout=10)
    missing = [
        NewTopic(
            t,
            num_partitions=1,
            replication_factor=1,
            config={"min.insync.replicas": "1"},
        )
        for t in topics
        if t not in md.topics
    ]
    if not missing:
        return
    futures = admin.create_topics(missing)
    for future in futures.values():
        future.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic_orders", default="order_created")
    parser.add_argument("--topic_payments", default="payment_confirmed")
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--users", type=int, default=50)
    parser.add_argument("--match_rate", type=float, default=0.7)
    parser.add_argument("--late_rate", type=float, default=0.2)
    parser.add_argument("--orphan_payment_rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    ensure_topics(args.bootstrap, [args.topic_orders, args.topic_payments])

    rng = random.Random(args.seed)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    base_sec = (int(time.time()) // 60) * 60
    base_ms = base_sec * 1000

    orders = []
    payments = []

    for i in range(args.orders):
        order_id = f"order_{i+1}"
        user_id = f"user_{i % args.users}"
        amount = round(rng.uniform(10, 500), 2)
        event_time_ms = base_ms + i * 60000
        orders.append(
            {
                "topic": args.topic_orders,
                "key": order_id,
                "payload": {
                    "event_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "user_id": user_id,
                    "amount": amount,
                    "event_time_ms": event_time_ms,
                },
            }
        )

        if rng.random() < args.match_rate:
            is_late = rng.random() < args.late_rate
            if is_late:
                delay_sec = rng.randint(70, 120)
            else:
                delay_sec = rng.randint(5, 40)
            pay_time_ms = event_time_ms + delay_sec * 1000
            payments.append(
                {
                    "topic": args.topic_payments,
                    "key": order_id,
                    "payload": {
                        "event_id": str(uuid.uuid4()),
                        "order_id": order_id,
                        "status": "confirmed",
                        "amount": amount,
                        "event_time_ms": pay_time_ms,
                    },
                }
            )

    orphan_count = int(args.orders * args.orphan_payment_rate)
    for j in range(orphan_count):
        order_id = f"order_orphan_{j+1}"
        event_time_ms = base_ms + (args.orders + j) * 60000
        payments.append(
            {
                "topic": args.topic_payments,
                "key": order_id,
                "payload": {
                    "event_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "status": "confirmed",
                    "amount": round(rng.uniform(10, 500), 2),
                    "event_time_ms": event_time_ms,
                },
            }
        )

    payments_by_order = {p["payload"]["order_id"]: p for p in payments}
    events = []
    for order in orders:
        events.append(order)
        payment = payments_by_order.get(order["payload"]["order_id"])
        if payment is not None:
            events.append(payment)
    for payment in payments:
        if payment["payload"]["order_id"].startswith("order_orphan_"):
            events.append(payment)

    sent_orders = 0
    sent_payments = 0

    for ev in events:
        sent_at_ms = int(time.time() * 1000)
        payload = dict(ev["payload"])
        payload["sent_at_ms"] = sent_at_ms
        producer.produce(
            ev["topic"],
            key=ev["key"].encode(),
            value=orjson.dumps(payload),
        )
        producer.poll(0)
        if ev["topic"] == args.topic_orders:
            sent_orders += 1
        else:
            sent_payments += 1

    producer.flush(5)

    print(
        f"sent_orders={sent_orders} sent_payments={sent_payments} "
        f"orders={args.orders} orphan_payments={orphan_count}"
    )


if __name__ == "__main__":
    main()
