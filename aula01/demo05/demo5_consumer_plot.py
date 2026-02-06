import argparse
import csv
import os
import time

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


def classify(event_time_ms, processing_time_ms, watermark_ms, on_time_ms):
    delay = processing_time_ms - event_time_ms
    if event_time_ms < watermark_ms:
        return "red"
    if delay > on_time_ms:
        return "orange"
    return "green"


def save_svg(points, path, tick_ms, width=900, height=500, pad=50):
    if not points:
        with open(path, "w", encoding="utf-8") as f:
            f.write("<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>")
        return

    min_x = min(p[0] for p in points)
    max_x = max(p[0] for p in points)
    min_y = min(p[1] for p in points)
    max_y = max(p[1] for p in points)
    if max_x == min_x:
        max_x += 1
    if max_y == min_y:
        max_y += 1

    def scale_x(x):
        return pad + (x - min_x) * (width - 2 * pad) / (max_x - min_x)

    def scale_y(y):
        return height - pad - (y - min_y) * (height - 2 * pad) / (max_y - min_y)

    colors = {"green": "#2ecc71", "orange": "#f39c12", "red": "#e74c3c"}
    grid = "#e6e6e6"
    label = "#555555"

    with open(path, "w", encoding="utf-8") as f:
        f.write(
            f"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\">"
        )
        f.write(
            f"<rect x=\"0\" y=\"0\" width=\"{width}\" height=\"{height}\" fill=\"#ffffff\"/>"
        )
        f.write(
            f"<line x1=\"{pad}\" y1=\"{height-pad}\" x2=\"{width-pad}\" y2=\"{height-pad}\" stroke=\"#333\"/>"
        )
        f.write(
            f"<line x1=\"{pad}\" y1=\"{pad}\" x2=\"{pad}\" y2=\"{height-pad}\" stroke=\"#333\"/>"
        )

        start_x = (min_x // tick_ms) * tick_ms
        end_x = ((max_x // tick_ms) + 1) * tick_ms
        start_y = (min_y // tick_ms) * tick_ms
        end_y = ((max_y // tick_ms) + 1) * tick_ms

        tick = start_x
        while tick <= end_x:
            x = scale_x(tick)
            f.write(
                f"<line x1=\"{x:.2f}\" y1=\"{pad}\" x2=\"{x:.2f}\" y2=\"{height-pad}\" stroke=\"{grid}\"/>"
            )
            if ((tick - start_x) // tick_ms) % 2 == 0:
                f.write(
                    f"<text x=\"{x:.2f}\" y=\"{height - pad + 18}\" font-size=\"10\" "
                    f"fill=\"{label}\" text-anchor=\"middle\">{int((tick - min_x) / 60000)}m</text>"
                )
            tick += tick_ms

        tick = start_y
        while tick <= end_y:
            y = scale_y(tick)
            f.write(
                f"<line x1=\"{pad}\" y1=\"{y:.2f}\" x2=\"{width-pad}\" y2=\"{y:.2f}\" stroke=\"{grid}\"/>"
            )
            if ((tick - start_y) // tick_ms) % 2 == 0:
                f.write(
                    f"<text x=\"{pad - 8}\" y=\"{y + 3:.2f}\" font-size=\"10\" "
                    f"fill=\"{label}\" text-anchor=\"end\">{int((tick - min_y) / 60000)}m</text>"
                )
            tick += tick_ms

        diag_min = min(min_x, min_y)
        diag_max = max(max_x, max_y)
        x1 = scale_x(diag_min)
        y1 = scale_y(diag_min)
        x2 = scale_x(diag_max)
        y2 = scale_y(diag_max)
        f.write(
            f"<line x1=\"{x1:.2f}\" y1=\"{y1:.2f}\" x2=\"{x2:.2f}\" y2=\"{y2:.2f}\" "
            f"stroke=\"#555\" stroke-dasharray=\"4,4\"/>"
        )

        for x, y, color in points:
            cx = scale_x(x)
            cy = scale_y(y)
            f.write(
                f"<circle cx=\"{cx:.2f}\" cy=\"{cy:.2f}\" r=\"3\" fill=\"{colors[color]}\"/>"
            )

        f.write(
            f"<text x=\"{width/2:.2f}\" y=\"{height-8}\" font-size=\"12\" "
            f"fill=\"{label}\" text-anchor=\"middle\">processing_time (min)</text>"
        )
        f.write(
            f"<text x=\"{12}\" y=\"{height/2:.2f}\" font-size=\"12\" "
            f"fill=\"{label}\" text-anchor=\"middle\" transform=\"rotate(-90 12 {height/2:.2f})\">"
            f"event_time (min)</text>"
        )
        f.write("</svg>")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.events.etpt")
    parser.add_argument("--group", default="demo4_plot")
    parser.add_argument("--records", type=int, default=50)
    parser.add_argument("--watermark_min", type=int, default=5)
    parser.add_argument("--window_min", type=int, default=10)
    parser.add_argument("--tick_min", type=int, default=1)
    parser.add_argument("--on_time_sec", type=int, default=60)
    parser.add_argument("--csv_path")
    parser.add_argument("--svg_path")
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
    tick_ms = args.tick_min * 60 * 1000
    on_time_ms = args.on_time_sec * 1000

    points = []
    max_event_time_ms = None
    counts = {"green": 0, "orange": 0, "red": 0}

    try:
        while len(points) < args.records:
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
            points.append((processing_time_ms, event_time_ms, color))

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = args.csv_path or os.path.join(base_dir, "points.csv")
    svg_path = args.svg_path or os.path.join(base_dir, "plot.svg")

    csv_dir = os.path.dirname(csv_path)
    svg_dir = os.path.dirname(svg_path)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)
    if svg_dir:
        os.makedirs(svg_dir, exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["processing_time_ms", "event_time_ms", "color"])
        for x, y, color in points:
            writer.writerow([x, y, color])

    save_svg(points, svg_path, tick_ms=tick_ms)

    print(f"green={counts['green']} orange={counts['orange']} red={counts['red']}")
    print(f"dlq_count={counts['red']}")
    print(f"csv={csv_path}")
    print(f"svg={svg_path}")


if __name__ == "__main__":
    main()
