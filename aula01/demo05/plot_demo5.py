import argparse
import json

import matplotlib.pyplot as plt
import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="demo5_data.json")
    parser.add_argument("--output", default="demo5_plot.png")
    args = parser.parse_args()

    with open(args.input, "r") as f:
        data_points = json.load(f)

    colors_map = {"green": "green", "orange": "orange", "red": "red"}

    fig, ax = plt.subplots(figsize=(10, 8))

    for color_name in ["green", "orange", "red"]:
        points = [p for p in data_points if p["color"] == color_name]
        if points:
            x = [p["processing_time_ms"] for p in points]
            y = [p["event_time_ms"] for p in points]
            ax.scatter(x, y, c=colors_map[color_name], label=color_name, s=100, alpha=0.7)

    # Linha de referência y = x
    all_times = []
    for p in data_points:
        all_times.append(p["processing_time_ms"])
        all_times.append(p["event_time_ms"])

    min_time = min(all_times)
    max_time = max(all_times)
    ax.plot([min_time, max_time], [min_time, max_time], "k--", label="y=x", linewidth=2)

    ax.set_xlabel("Processing Time (ms)", fontsize=12)
    ax.set_ylabel("Event Time (ms)", fontsize=12)
    ax.set_title("Event-time vs Processing-time", fontsize=14)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(args.output, dpi=150, format=args.output.split(".")[-1].lower())
    print(f"gráfico salvo em {args.output}")


if __name__ == "__main__":
    main()
