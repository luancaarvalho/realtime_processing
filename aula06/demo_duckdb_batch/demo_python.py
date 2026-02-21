from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "small_data.duckdb"


QUERIES = {
    "show_tables": "SHOW TABLES;",
    "sample_bronze": "SELECT * FROM bronze_sales ORDER BY event_time LIMIT 8;",
    "kpis": "SELECT * FROM gold_kpis ORDER BY gmv_total DESC;",
    "gmv_by_channel": """
        SELECT channel, SUM(gmv) AS gmv_total
        FROM bronze_sales
        GROUP BY 1
        ORDER BY gmv_total DESC;
    """,
}


def print_block(title: str, rows) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    for row in rows:
        print(row)


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(
            f"Banco nao encontrado em {DB_PATH}. Rode primeiro: python setup_demo.py"
        )

    con = duckdb.connect(str(DB_PATH))
    try:
        for name, query in QUERIES.items():
            rows = con.execute(query).fetchall()
            print_block(name, rows)
    finally:
        con.close()


if __name__ == "__main__":
    main()
