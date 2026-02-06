import argparse
import time

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


STATIC_TABLE = {
    "user_0": {"segment": "gold", "country": "BR"},
    "user_1": {"segment": "silver", "country": "BR"},
    "user_2": {"segment": "gold", "country": "US"},
    "user_3": {"segment": "bronze", "country": "BR"},
    "user_4": {"segment": "gold", "country": "PT"},
    "user_5": {"segment": "silver", "country": "BR"},
    "user_6": {"segment": "bronze", "country": "US"},
    "user_7": {"segment": "gold", "country": "BR"},
    "user_8": {"segment": "silver", "country": "PT"},
    "user_9": {"segment": "gold", "country": "BR"},
    "user_10": {"segment": "silver", "country": "US"},
    "user_11": {"segment": "bronze", "country": "BR"},
    "user_12": {"segment": "gold", "country": "BR"},
    "user_13": {"segment": "silver", "country": "PT"},
    "user_14": {"segment": "bronze", "country": "US"},
    "user_15": {"segment": "gold", "country": "BR"},
    "user_16": {"segment": "silver", "country": "BR"},
    "user_17": {"segment": "gold", "country": "US"},
    "user_18": {"segment": "bronze", "country": "BR"},
    "user_19": {"segment": "gold", "country": "PT"},
}


def print_table(table, limit):
    keys = list(table.keys())
    print(f"joined_table size={len(keys)}")
    for k in keys[:limit]:
        print(f"{k} {orjson.dumps(table[k]).decode()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic_orders", default="order_created")
    parser.add_argument("--group", default="demo4_join_static")
    parser.add_argument("--ttl_sec", type=int, default=60)
    parser.add_argument("--print_every", type=int, default=200)
    parser.add_argument("--print_limit", type=int, default=10)
    parser.add_argument("--max_idle_sec", type=int, default=5)
    args = parser.parse_args()

    print("static_table", orjson.dumps(STATIC_TABLE).decode())

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic_orders])

    ttl_ms = args.ttl_sec * 1000
    joined = {}
    pending = {}
    dlq = []
    processed = 0
    last_msg_time = time.time()

    def evict(now_ms):
        to_drop = [k for k, v in pending.items() if now_ms - v["arrival_ms"] > ttl_ms]
        for k in to_drop:
            dlq.append(pending.pop(k))

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_msg_time >= args.max_idle_sec:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_msg_time = time.time()
            data = orjson.loads(msg.value())
            order_id = data.get("order_id")
            user_id = data.get("user_id")
            amount = data.get("amount")
            event_time_ms = data.get("event_time_ms")
            if order_id is None or user_id is None or amount is None or event_time_ms is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_time_ms = int(event_time_ms)
            now_ms = int(time.time() * 1000)

            user_in_database = check_user_postgres(user_id)
            if user_in_database.is_in_database:
                joined[order_id] = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "amount": float(amount),
                    "event_time_ms": event_time_ms,
                    "segment": STATIC_TABLE[user_id]["segment"],
                    "country": STATIC_TABLE[user_id]["country"],
                }
            else:
                pending[order_id] = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "amount": float(amount),
                    "event_time_ms": event_time_ms,
                    "arrival_ms": now_ms,
                    "eval": user_in_database.log
                }

            processed += 1
            evict(now_ms)

            if processed % args.print_every == 0:
                print_table(joined, args.print_limit)

            consumer.commit(message=msg, asynchronous=False)
    finally:
        evict(int(time.time() * 1000) + ttl_ms + 1)
        consumer.close()

    print_table(joined, args.print_limit)
    print(f"dlq_count={len(dlq)}")


if __name__ == "__main__":
    main()
