import json, time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import matplotlib.pyplot as plt
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "demo5-consumer-latency"
TOPIC = "demo5-topic"

AUTO_OFFSET_RESET = "earliest"

WATERMARK_THRESHOLD = pd.Timedelta("10min")
ACCEPTABLE_LATENCY = pd.Timedelta("30s")

MAX_MESSAGES = 500
MAX_SECONDS = 30

OUTPUT_FILE = Path.cwd() / "event_vs_processing_time.png"


def build_consumer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": True,
    }
    return Consumer(conf)


def parse_event_time(payload):
    try:
        return pd.to_datetime(payload["event_time"], utc=True)
    except Exception:
        return None


def consume():
    consumer = build_consumer()
    consumer.subscribe([TOPIC])

    rows = []
    start = time.time()

    print("📥 Consumer iniciado, aguardando mensagens...")

    try:
        while True:
            if len(rows) >= MAX_MESSAGES:
                print("📦 Limite de mensagens atingido")
                break

            if time.time() - start > MAX_SECONDS:
                print("⏰ Tempo máximo de consumo atingido")
                break

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"❌ Erro no consumer: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                print("⚠️ Payload inválido, ignorando mensagem")
                continue

            event_time = parse_event_time(payload)
            if event_time is None:
                print("⚠️ event_time ausente ou inválido")
                continue

            processing_time = datetime.now(timezone.utc)
            lateness = processing_time - event_time

            if lateness <= ACCEPTABLE_LATENCY:
                status = "On-time"
            elif lateness <= WATERMARK_THRESHOLD:
                status = "Late"
            else:
                status = "DLQ/Dropped"

            print(
                f"📨 Consumido | "
                f"event_time={event_time.isoformat()} | "
                f"processing_time={processing_time.isoformat()} | "
                f"lateness={int(lateness.total_seconds())}s | "
                f"status={status}"
            )

            rows.append(
                {
                    "event_time": event_time,
                    "processing_time": processing_time,
                }
            )

    finally:
        consumer.close()
        print("🔒 Consumer finalizado")

    return pd.DataFrame(rows)


def classify(df):
    df["lateness"] = df["processing_time"] - df["event_time"]

    def status(l):
        if l <= ACCEPTABLE_LATENCY:
            return "On-time"
        if l <= WATERMARK_THRESHOLD:
            return "Late"
        return "DLQ/Dropped"

    df["status"] = df["lateness"].apply(status)
    return df


def plot(df):
    colors = {
        "On-time": "green",
        "Late": "orange",
        "DLQ/Dropped": "red",
    }

    fig, ax = plt.subplots(figsize=(11, 7))

    for status, g in df.groupby("status"):
        ax.scatter(
            g["processing_time"],
            g["event_time"],
            color=colors[status],
            label=f"{status} (n={len(g)})",
            s=28,
            alpha=0.85,
        )

    min_ts = min(df["event_time"].min(), df["processing_time"].min())
    max_ts = max(df["event_time"].max(), df["processing_time"].max())

    ax.plot(
        [min_ts, max_ts],
        [min_ts, max_ts],
        linestyle="--",
        color="gray",
        label="Ideal (y=x)",
    )

    ax.set_xlabel("processing_time (UTC)")
    ax.set_ylabel("event_time (UTC)")
    ax.set_title(
        f"Event-time vs Processing-time\n"
        f"ACCEPTABLE_LATENCY={ACCEPTABLE_LATENCY}, "
        f"WATERMARK={WATERMARK_THRESHOLD}"
    )
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.autofmt_xdate(rotation=25)

    plt.tight_layout()
    fig.savefig(OUTPUT_FILE, dpi=160)
    plt.close(fig)

    print(f"📊 Gráfico salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    df_raw = consume()

    if df_raw.empty:
        print("⚠️ Nenhuma mensagem válida consumida")
    else:
        df = classify(df_raw)

        print("\n📈 Resumo final:")
        print(df["status"].value_counts())

        plot(df)
