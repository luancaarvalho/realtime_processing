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
                num_partitions=1,
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
    parser.add_argument("--topic", default="rt.events.etpt")
    parser.add_argument("--records", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--window_min", type=int, default=10)
    parser.add_argument("--watermark_min", type=int, default=5)
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)

    rng = random.Random(args.seed)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    window_ms = args.window_min * 60 * 1000
    watermark_ms = args.watermark_min * 60 * 1000
    base_sec = (int(time.time()) // (args.window_min * 60)) * (args.window_min * 60)
    base_ms = base_sec * 1000

    step_ms = max(1, window_ms // args.records)
    events = []

    for i in range(args.records):
        event_time_ms = base_ms + i * step_ms
        r = rng.random()
        if r < 0.7:
            delay_ms = rng.randint(0, 30_000)
        elif r < 0.9:
            delay_ms = rng.randint(90_000, min(240_000, watermark_ms - 60_000))
        else:
            delay_ms = rng.randint(watermark_ms + 60_000, watermark_ms + 180_000)
        processing_time_ms = event_time_ms + delay_ms
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_time_ms": event_time_ms,
                "processing_time_ms": processing_time_ms,
                "delay_ms": delay_ms,
            }
        )

    events.sort(key=lambda e: e["processing_time_ms"])

    for ev in events:
        producer.produce(
            args.topic,
            key=str(ev["event_id"]).encode(),
            value=orjson.dumps(ev),
        )
        producer.poll(0)

    producer.flush(5)
    print(f"sent={len(events)} seed={args.seed} topic={args.topic}")


if __name__ == "__main__":
    main()
