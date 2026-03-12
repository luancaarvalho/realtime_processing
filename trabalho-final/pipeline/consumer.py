from pathlib import Path
import json

import pandas as pd
from kafka import KafkaConsumer

from transformations import transform_events
from aggregations import (
    aggregate_metrics,
    aggregate_by_seller,
    aggregate_by_region,
    aggregate_top_products,
)
from storage import save_metrics, append_events_history


HISTORY_FILE = Path("trabalho-final/data/events_history.jsonl")

consumer = KafkaConsumer(
    "blackfriday_orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

batch = []

print("Consumer iniciado. Aguardando eventos...")


for message in consumer:
    batch.append(message.value)

    if len(batch) >= 50:
        df_batch = pd.DataFrame(batch)
        df_batch = transform_events(df_batch)

        append_events_history(df_batch)

        history_df = pd.read_json(HISTORY_FILE, lines=True)
        history_df = transform_events(history_df)

        metrics = aggregate_metrics(history_df)
        metrics["by_seller"] = aggregate_by_seller(history_df)
        metrics["by_region"] = aggregate_by_region(history_df)
        metrics["top_products"] = aggregate_top_products(history_df)

        save_metrics(metrics)

        print("Métricas acumuladas salvas com sucesso.")

        batch = []