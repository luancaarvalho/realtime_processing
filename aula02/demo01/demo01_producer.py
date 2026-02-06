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
    parser.add_argument("--topic", default="rt.files.raw")
    parser.add_argument("--files_per_min", type=int, default=100)
    parser.add_argument("--minutes", type=int, default=100)
    parser.add_argument("--minute_sleep_sec", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)

    rng = random.Random(args.seed)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    base_sec = (int(time.time()) // 60) * 60
    base_ms = base_sec * 1000
    total = 0

    for minute in range(args.minutes):
        minute_start_ms = base_ms + minute * 60_000
        for i in range(args.files_per_min):
            event_time_ms = minute_start_ms + int(i * (60_000 / args.files_per_min))
            payload = {
                "file_id": str(uuid.uuid4()),
                "user_id": f"user_{rng.randint(1, 50)}",
                "size": rng.randint(10, 5000),
                "event_time_ms": event_time_ms,
                "sent_at_ms": int(time.time() * 1000),
            }
            producer.produce(
                args.topic,
                key=payload["user_id"].encode(),
                value=orjson.dumps(payload),
            )
            producer.poll(0)
            total += 1
        if args.minute_sleep_sec > 0:
            time.sleep(args.minute_sleep_sec)
        if (minute + 1) % 5 == 0:
            print(f"minutes={minute+1} total={total}")

    producer.flush(5)
    print(f"sent={total}")


if __name__ == "__main__":
    main()
