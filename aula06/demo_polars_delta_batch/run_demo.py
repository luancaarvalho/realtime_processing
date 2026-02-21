from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
BRONZE = DATA_DIR / "bronze_sales_delta"
GOLD = DATA_DIR / "gold_kpis_delta"


def build_batch_sales() -> pl.DataFrame:
    base = datetime(2026, 2, 20, 14, 0, 0)
    rows = []
    sellers = ["seller_a", "seller_b", "seller_c"]
    channels = ["web", "mobile", "marketplace"]

    for i in range(1, 31):
        seller = sellers[i % len(sellers)]
        channel = channels[i % len(channels)]
        price = float(40 + (i % 7) * 10)
        qty = int(1 + (i % 4))
        rows.append(
            {
                "event_id": f"ev_{i:03d}",
                "event_time": base + timedelta(minutes=i),
                "seller_id": seller,
                "channel": channel,
                "sku": f"sku_{(i % 5) + 1}",
                "price": price,
                "qty": qty,
            }
        )

    return pl.DataFrame(rows).with_columns((pl.col("price") * pl.col("qty")).alias("gmv"))


def print_section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df = build_batch_sales()

    print_section("1) Batch de entrada (DataFrame Polars)")
    print(df.head(8))
    print("shape:", df.shape)

    print_section("2) Escrita em Delta (bronze)")
    df.write_delta(str(BRONZE), mode="overwrite")
    print(f"Tabela bronze criada em: {BRONZE}")

    print_section("3) Leitura da Delta bronze (polars.read_delta)")
    bronze = pl.read_delta(str(BRONZE))
    print(bronze.head(8))
    print("linhas bronze:", bronze.height)

    print_section("4) Agregacao batch para gerar gold")
    gold = (
        bronze.lazy()
        .group_by(["seller_id", "channel"])
        .agg(
            [
                pl.sum("gmv").alias("gmv_total"),
                pl.mean("gmv").alias("ticket_medio"),
                pl.len().alias("eventos"),
            ]
        )
        .sort(["gmv_total"], descending=True)
        # .collect()
    )
    # print(gold)
    gold.show()

    print_section("5) Escrita da tabela gold")
    gold.write_delta(str(GOLD), mode="overwrite")
    print(f"Tabela gold criada em: {GOLD}")

    print_section("6) Leitura final da gold")
    final_gold = pl.read_delta(str(GOLD))
    print(final_gold)

    print_section("Comandos uteis para explorar")
    print(f"pl.read_delta('{BRONZE}').head(10)")
    print(f"pl.read_delta('{GOLD}').sort('gmv_total', descending=True)")


if __name__ == "__main__":
    # pl.read_delta('/Users/luancarvalho/Documents/Unifor/realtime_processing/aula06/demo_polars_delta_batch/data/gold_kpis_delta/').head(10)
    main()
