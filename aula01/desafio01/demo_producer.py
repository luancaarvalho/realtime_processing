import json, random, time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo5-topic"

BASE_DELAY_SECONDS = 2
MAX_EXTRA_DELAY_SECONDS = 900


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Erro ao enviar mensagem: {err}")
    else:
        print(
            f"✅ Enviado | "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def build_producer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 10,
    }
    return Producer(conf)


def generate_event_time():
    now = datetime.now(timezone.utc)
    extra_delay = random.choice(
        [
            random.randint(0, 20),
            random.randint(40, 300),
            random.randint(600, MAX_EXTRA_DELAY_SECONDS),
        ]
    )
    return now - timedelta(seconds=extra_delay)


def main():
    producer = build_producer()

    try:
        while True:
            event_time = generate_event_time()

            payload = {
                "event_time": event_time.isoformat(),
            }

            print(f"📤 Produzindo evento | event_time={payload['event_time']}")

            producer.produce(
                topic=TOPIC,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=delivery_report,
            )

            producer.poll(0)
            time.sleep(BASE_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Producer interrompido pelo usuário")

    finally:
        print("🚿 Enviando mensagens pendentes...")
        producer.flush()
        print("✅ Producer finalizado")


if __name__ == "__main__":
    main()
