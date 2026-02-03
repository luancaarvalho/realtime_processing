import json
import time
import matplotlib.pyplot as plt


def classify(proc_ms: int, event_ms: int, watermark_ms: int = 60_000):
    """
    Classifica evento baseado no atraso (proc - event).
    - green: on-time (delay <= 0)
    - orange: late mas dentro da watermark (0 < delay <= watermark)
    - red: fora da watermark (delay > watermark)
    """
    delay = proc_ms - event_ms
    if delay <= 0:
        return "green"
    if delay <= watermark_ms:
        return "orange"
    return "red"


def ensure_pairs_format(payload: dict):
    """
    Retorna um dict no formato:
    {"green":[(proc,event)], "orange":[...], "red":[...]}
    Aceita:
    1) payload já nesse formato
    2) payload do consumer em lote: {"batch":..., "events":[{...}, ...]}
    """
    # Caso 1: já está no formato esperado
    if any(k in payload for k in ("green", "orange", "red")):
        out = {
            "green": payload.get("green", []),
            "orange": payload.get("orange", []),
            "red": payload.get("red", []),
        }
        return out

    # Caso 2: formato batch (a gente gerou assim no consumer)
    events_list = payload.get("events")
    if not isinstance(events_list, list):
        raise ValueError(
            "Formato inesperado do JSON. Esperado chaves 'green/orange/red' "
            "ou 'events' (lista) dentro do arquivo."
        )

    out = {"green": [], "orange": [], "red": []}

    now_ms = int(time.time() * 1000)
    for ev in events_list:
        if not isinstance(ev, dict):
            continue

        event_ms = ev.get("event_time_ms")
        if event_ms is None:
            continue

        # Se seu producer incluir processing_time_ms, usamos.
        # Senão, usa "agora" (é só para plot; se quiser mais fiel, grave proc_time no consumer)
        proc_ms = ev.get("processing_time_ms", now_ms)

        color = classify(int(proc_ms), int(event_ms), watermark_ms=60_000)
        out[color].append((int(proc_ms), int(event_ms)))

    return out


def main():
    try:
        with open("events_batch.json", "r", encoding="utf-8") as f:
            payload = json.load(f)
    except FileNotFoundError:
        print("Erro: events_batch.json não encontrado!")
        print("Execute primeiro o consumer.py para gerar os dados.")
        return

    try:
        events = ensure_pairs_format(payload)
    except ValueError as e:
        print(f"Erro ao interpretar JSON: {e}")
        return

    fig, ax = plt.subplots(figsize=(12, 8))

    if events["green"]:
        xg, yg = zip(*events["green"])
        ax.scatter(xg, yg, c="green", label="On-time", alpha=0.6, s=50)

    if events["orange"]:
        xo, yo = zip(*events["orange"])
        ax.scatter(
            xo, yo, c="orange", label="Late (dentro da watermark)", alpha=0.6, s=50
        )

    if events["red"]:
        xr, yr = zip(*events["red"])
        ax.scatter(xr, yr, c="red", label="DLQ", alpha=0.6, s=50)

    # Linha y=x (referência)
    all_vals = []
    for pairs in (events["green"], events["orange"], events["red"]):
        for proc_time, event_time in pairs:
            all_vals.extend([proc_time, event_time])

    if all_vals:
        min_val = min(all_vals)
        max_val = max(all_vals)
        ax.plot(
            [min_val, max_val],
            [min_val, max_val],
            "k--",
            linewidth=2,
            label="y=x (referência)",
            alpha=0.5,
        )

    ax.set_xlabel("Processing Time (ms)", fontsize=12)
    ax.set_ylabel("Event Time (ms)", fontsize=12)
    ax.set_title("Processing Time vs Event Time", fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.savefig("scatter_plot.png", dpi=300, bbox_inches="tight")
    plt.savefig("scatter_plot.svg", bbox_inches="tight")

    print("Gráficos salvos: scatter_plot.png e scatter_plot.svg")
    print("\nResumo dos eventos:")
    print(f"  Green (on-time): {len(events['green'])}")
    print(f"  Orange (late):   {len(events['orange'])}")
    print(f"  Red (DLQ):       {len(events['red'])}")


if __name__ == "__main__":
    main()
