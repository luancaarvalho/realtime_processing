import argparse
import random
import time
import uuid

import orjson
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(bootstrap, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        return
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=3,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        ]
    )
    for future in futures.values():
        future.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.sales.raw")
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--late_rate", type=float, default=0.08)
    parser.add_argument("--late_max_sec", type=int, default=120)
    parser.add_argument("--dup_rate", type=float, default=0.02)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)

    rng = random.Random(args.seed)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    interval = 1.0 / max(args.rate, 1.0)
    next_send = time.perf_counter()
    last_event = None
    dup_total = 0

    for i in range(args.records):
        now_ms = int(time.time() * 1000)
        if last_event is not None and rng.random() < args.dup_rate:
            event = last_event
            dup_total += 1
        else:
            if rng.random() < args.late_rate:
                offset_ms = int(rng.uniform(1, max(args.late_max_sec, 1)) * 1000)
                event_time_ms = now_ms - offset_ms
            else:
                event_time_ms = now_ms
            event = {
                "event_id": str(uuid.uuid4()),
                "symbol": rng.choice(symbols),
                "price": round(rng.uniform(10, 50000), 2),
                "qty": round(rng.uniform(0.001, 5.0), 6),
                "event_time_ms": event_time_ms,
                "sent_at_ms": now_ms,
            }
            last_event = event

        producer.produce(
            args.topic,
            key=event["symbol"].encode(),
            value=orjson.dumps(event),
        )
        producer.poll(0)

        next_send += interval
        sleep_for = next_send - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush(5)
    print(f"sent={args.records} dup={dup_total}")


if __name__ == "__main__":
    main()
