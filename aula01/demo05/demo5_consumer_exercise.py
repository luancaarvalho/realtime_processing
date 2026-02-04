import argparse
import time
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


def classify(event_time_ms, processing_time_ms, watermark_ms, on_time_ms):
    delay = processing_time_ms - event_time_ms
    if event_time_ms < watermark_ms:
        return "red"
    if delay > on_time_ms:
        return "orange"
    return "green"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.events.etpt")
    parser.add_argument("--group", default="demo5_exercise")
    parser.add_argument("--records", type=int, default=50)
    parser.add_argument("--watermark_min", type=int, default=5)
    parser.add_argument("--window_min", type=int, default=10)
    parser.add_argument("--on_time_sec", type=int, default=60)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    watermark_ms = args.watermark_min * 60 * 1000
    on_time_ms = args.on_time_sec * 1000

    max_event_time_ms = None
    counts = {"green": 0, "orange": 0, "red": 0}
    xs, ys, cs = [], [], []

    try:
        processed = 0
        while processed < args.records:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            data = orjson.loads(msg.value())
            event_time_ms = data.get("event_time_ms")
            processing_time_ms = data.get("processing_time_ms")
            if event_time_ms is None or processing_time_ms is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_time_ms = int(event_time_ms)
            processing_time_ms = int(processing_time_ms)

            if max_event_time_ms is None or event_time_ms > max_event_time_ms:
                max_event_time_ms = event_time_ms

            current_watermark = max_event_time_ms - watermark_ms
            color = classify(event_time_ms, processing_time_ms, current_watermark, on_time_ms)
            counts[color] += 1
            # collect for plotting (per-record)
            xs.append(processing_time_ms)
            ys.append(event_time_ms)
            cs.append(color)
            processed += 1

            consumer.commit(message=msg, asynchronous=False)

    finally:
        consumer.close()

    print(f"green={counts['green']} orange={counts['orange']} red={counts['red']}")
    print(f"dlq_count={counts['red']}")

    x_dt = [datetime.fromtimestamp(ms / 1000.0) for ms in xs]
    y_dt = [datetime.fromtimestamp(ms / 1000.0) for ms in ys]

    labels = ['on-time' if c == 'green' else 'late' if c == 'orange' else 'DLQ' for c in cs]
    palette = {'on-time': 'green', 'late': 'orange', 'DLQ': 'red'}

    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=x_dt, y=y_dt, hue=labels, palette=palette, s=50, alpha=0.85)

    # Linha central y=x
    min_dt = min(min(x_dt), min(y_dt))
    max_dt = max(max(x_dt), max(y_dt))
    plt.plot([min_dt, max_dt], [min_dt, max_dt], '--', color='k', linewidth=1)

    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.yaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.gcf().autofmt_xdate()

    plt.xlabel('processing_time')
    plt.ylabel('event_time')
    plt.title('Event-time vs Processing-time')
    plt.savefig('event_vs_processing.png', dpi=150, bbox_inches='tight')
    print('Saved plot to event_vs_processing.png')



if __name__ == "__main__":
    main()
