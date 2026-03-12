import json
from pathlib import Path
from datetime import datetime


OUTPUT_FILE = Path("trabalho-final/data/metrics.json")
HISTORY_FILE = Path("trabalho-final/data/events_history.jsonl")


def _to_python(value):
    if hasattr(value, "item"):
        return value.item()
    return value


def save_metrics(metrics: dict):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    clean_metrics = {}
    for key, value in metrics.items():
        if isinstance(value, list):
            clean_metrics[key] = [
                {inner_k: _to_python(inner_v) for inner_k, inner_v in row.items()}
                for row in value
            ]
        else:
            clean_metrics[key] = _to_python(value)

    clean_metrics["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    temp_file = OUTPUT_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(clean_metrics, f, indent=2, ensure_ascii=False, default=str)

    temp_file.replace(OUTPUT_FILE)


def append_events_history(df):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        for row in df.to_dict(orient="records"):
            clean_row = {k: _to_python(v) for k, v in row.items()}
            f.write(json.dumps(clean_row, ensure_ascii=False, default=str) + "\n")