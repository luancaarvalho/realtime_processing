import argparse
import time

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


def print_table(table, limit):
    keys = list(table.keys())
    print(f"joined_table size={len(keys)}")
    for k in keys[:limit]:
        print(f"{k} {orjson.dumps(table[k]).decode()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic_orders", default="order_created")
    parser.add_argument("--topic_payments", default="payment_confirmed")
    parser.add_argument("--group", default="demo4_join_stream")
    parser.add_argument("--ttl_sec", type=int, default=60)
    parser.add_argument("--print_every", type=int, default=200)
    parser.add_argument("--print_limit", type=int, default=10)
    parser.add_argument("--max_idle_sec", type=int, default=5)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic_orders, args.topic_payments])

    ttl_ms = args.ttl_sec * 1000
    orders = {}
    payments = {}
    joined = {}
    dlq = []
    processed = 0
    last_msg_time = time.time()

    def try_join(order, payment):
        diff = abs(payment["event_time_ms"] - order["event_time_ms"])
        return diff <= ttl_ms

    def evict(now_ms):
        for key in [k for k, v in orders.items() if now_ms - v["arrival_ms"] > ttl_ms]:
            dlq.append(orders.pop(key))
        for key in [k for k, v in payments.items() if now_ms - v["arrival_ms"] > ttl_ms]:
            dlq.append(payments.pop(key))

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
            event_time_ms = data.get("event_time_ms")
            if order_id is None or event_time_ms is None:
                consumer.commit(message=msg, asynchronous=False)
                continue
            event_time_ms = int(event_time_ms)

            now_ms = int(time.time() * 1000)

            if msg.topic() == args.topic_orders:
                orders[order_id] = {
                    "order_id": order_id,
                    "user_id": data.get("user_id"),
                    "amount": float(data.get("amount", 0.0)),
                    "event_time_ms": event_time_ms,
                    "arrival_ms": now_ms,
                }
                if order_id in payments and try_join(orders[order_id], payments[order_id]):
                    joined[order_id] = {
                        "order_id": order_id,
                        "order_time_ms": orders[order_id]["event_time_ms"],
                        "payment_time_ms": payments[order_id]["event_time_ms"],
                        "amount": orders[order_id]["amount"],
                    }
                    orders.pop(order_id, None)
                    payments.pop(order_id, None)
            else:
                payments[order_id] = {
                    "order_id": order_id,
                    "status": data.get("status"),
                    "amount": float(data.get("amount", 0.0)),
                    "event_time_ms": event_time_ms,
                    "arrival_ms": now_ms,
                }
                if order_id in orders and try_join(orders[order_id], payments[order_id]):
                    joined[order_id] = {
                        "order_id": order_id,
                        "order_time_ms": orders[order_id]["event_time_ms"],
                        "payment_time_ms": payments[order_id]["event_time_ms"],
                        "amount": orders[order_id]["amount"],
                    }
                    orders.pop(order_id, None)
                    payments.pop(order_id, None)

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
