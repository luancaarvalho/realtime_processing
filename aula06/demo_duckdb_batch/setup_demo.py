from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "small_data.duckdb"
RAW_PARQUET = DATA_DIR / "raw_sales.parquet"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("DROP TABLE IF EXISTS bronze_sales")
        con.execute("DROP TABLE IF EXISTS gold_kpis")

        con.execute(
            """
            CREATE TABLE bronze_sales AS
            SELECT
                printf('duck_ev_%03d', i) AS event_id,
                TIMESTAMP '2026-02-20 15:00:00' + (i * INTERVAL '1 minute') AS event_time,
                CASE i % 3
                    WHEN 0 THEN 'seller_a'
                    WHEN 1 THEN 'seller_b'
                    ELSE 'seller_c'
                END AS seller_id,
                CASE i % 3
                    WHEN 0 THEN 'web'
                    WHEN 1 THEN 'mobile'
                    ELSE 'marketplace'
                END AS channel,
                printf('sku_%d', (i % 6) + 1) AS sku,
                CAST(30 + (i % 9) * 12 AS DOUBLE) AS price,
                CAST(1 + (i % 5) AS INTEGER) AS qty,
                CAST((30 + (i % 9) * 12) * (1 + (i % 5)) AS DOUBLE) AS gmv
            FROM range(1, 41) AS t(i);
            """
        )

        con.execute(
            """
            CREATE TABLE gold_kpis AS
            SELECT
                seller_id,
                channel,
                SUM(gmv) AS gmv_total,
                AVG(gmv) AS ticket_medio,
                COUNT(*) AS eventos
            FROM bronze_sales
            GROUP BY 1, 2
            ORDER BY gmv_total DESC;
            """
        )

        con.execute(
            "COPY bronze_sales TO ? (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)",
            [str(RAW_PARQUET)],
        )

        total = con.execute("SELECT COUNT(*) FROM bronze_sales").fetchone()[0]
        print(f"Banco criado: {DB_PATH}")
        print(f"Parquet criado: {RAW_PARQUET}")
        print(f"Tabela bronze_sales: {total} linhas")
        print("Tabela gold_kpis: criada")
    finally:
        con.close()


if __name__ == "__main__":
    main()
