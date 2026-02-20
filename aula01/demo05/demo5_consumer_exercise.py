import argparse

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException
import matplotlib.pyplot as plt


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

    # saída do gráfico
    parser.add_argument("--out", default="grafico.png")
    parser.add_argument("--dpi", type=int, default=200)

    # alterna tempo relativo vs absoluto
    parser.add_argument(
        "--relative-time",
        action="store_true",
        help="Usa segundos relativos a t0 (menor timestamp observado)",
    )

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
    # armazenar pontos
    xs = []  # processing_time_ms
    ys = []  # event_time_ms
    cs = []  # cor

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
            processed += 1
            xs.append(processing_time_ms)
            ys.append(event_time_ms)
            cs.append(color)

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()

    # ======================
    # Preparar eixos (absoluto vs relativo)
    # ======================
    if not xs or not ys:
        raise RuntimeError("Nenhum ponto coletado para plotar (tópico vazio?)")

    if args.relative_time:
        t0 = min(min(xs), min(ys))
        xs_plot = [(x - t0) / 1000 for x in xs]  # segundos
        ys_plot = [(y - t0) / 1000 for y in ys]  # segundos
        xlabel = "processing_time (s) desde t0"
        ylabel = "event_time (s) desde t0"
        title_suffix = " (segundos relativos)"
    else:
        xs_plot = xs  # ms
        ys_plot = ys  # ms
        xlabel = "processing_time (ms)"
        ylabel = "event_time (ms)"
        title_suffix = " (timestamps absolutos)"

    # ======================
    # Gráfico
    # ======================
    fig, ax = plt.subplots(figsize=(8, 6))

    for label, color in [
        ("on-time", "green"),
        ("late (within watermark)", "orange"),
        ("DLQ", "red"),
    ]:
        idx = [i for i, c in enumerate(cs) if c == color]
        if idx:
            ax.scatter(
                [xs_plot[i] for i in idx],
                [ys_plot[i] for i in idx],
                s=22,
                alpha=0.8,
                label=label,
                c=color,
            )

    # linha y = x
    mn = min(min(xs_plot), min(ys_plot))
    mx = max(max(xs_plot), max(ys_plot))
    ax.plot([mn, mx], [mn, mx], linestyle="--", linewidth=1.5)

    ax.set_title("processing_time (x) vs event_time (y)" + title_suffix)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.25)

    fig.tight_layout()
    fig.savefig(args.out, dpi=args.dpi)

    print(f"saved_plot={args.out}")


if __name__ == "__main__":
    main()