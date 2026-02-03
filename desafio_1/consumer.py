import argparse
import time
import orjson
from confluent_kafka import Consumer, KafkaException, KafkaError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="demo1_naive")
    parser.add_argument("--topic", default="rt.events.raw")
    parser.add_argument("--out", default="events_batch.json", help="Arquivo JSON (vai sobrescrever a cada lote)")
    parser.add_argument("--batch-size", type=int, default=100, help="Quantas mensagens por lote")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])

    buffer = []
    batch_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            data = orjson.loads(msg.value())
            buffer.append(data)

            # Quando bater 10 consumos, gera o arquivo e continua
            if len(buffer) >= args.batch_size:
                batch_count += 1

                payload = {
                    "batch": batch_count,
                    "batch_size": len(buffer),
                    "generated_at_ms": int(time.time() * 1000),
                    "events": buffer,
                }

                # sobrescreve o arquivo sempre
                with open(args.out, "wb") as f:
                    f.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

                print(f"✅ Lote {batch_count} salvo em {args.out} (sobrescrito)")

                buffer.clear()

    except KeyboardInterrupt:
        print("Interrompido.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
