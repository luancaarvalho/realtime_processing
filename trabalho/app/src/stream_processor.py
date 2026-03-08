from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import polars as pl
import psycopg2
import psycopg2.extras

from db_init import init_schema, wait_for_postgres
from kafka_utils import wait_for_kafka
from settings import SETTINGS

# ---------------------------------------------------------------------------
# Conexão PostgreSQL
# ---------------------------------------------------------------------------


def _pg_connect() -> psycopg2.extensions.connection:
    conn = wait_for_postgres()
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Leitura da fonte de verdade
# ---------------------------------------------------------------------------


def _load_raw_events(conn: psycopg2.extensions.connection) -> pl.DataFrame:
    """Carrega todos os eventos de raw_events em um DataFrame Polars."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM raw_events")
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        return pl.DataFrame()

    return pl.from_dicts([dict(zip(cols, row)) for row in rows])


# ---------------------------------------------------------------------------
# Toda a lógica de negócio — Polars puro
# ---------------------------------------------------------------------------


def _compute_all_kpis(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Recebe o DataFrame completo de raw_events e retorna todos os KPIs
    já calculados e prontos para gravação. Nenhuma regra de negócio
    fica fora desta função.

    Decisões:
    - Apenas eventos com status=PAID entram nos KPIs financeiros.
    - Taxas de cancelamento/devolução são calculadas sobre o total de eventos.
    - Margens, médias e rankings são derivados aqui para que o Metabase
      execute apenas SELECT nas colunas prontas.
    """
    paid = df.filter(pl.col("status") == "PAID")
    total_count = df.height

    # ── C-Level ─────────────────────────────────────────────────────────────
    if paid.is_empty():
        clevel = pl.DataFrame(
            {
                "gmv": [0.0],
                "net_profit": [0.0],
                "avg_ticket": [0.0],
                "total_orders": [0],
                "total_discount": [0.0],
                "cancellation_rate_pct": [0.0],
                "return_rate_pct": [0.0],
            }
        )
    else:
        cancelled = df.filter(pl.col("status") == "CANCELLED").height
        returned = df.filter(pl.col("status") == "RETURNED").height

        clevel = paid.select(
            [
                pl.sum("gross_amount").alias("gmv"),
                (pl.sum("net_revenue") - pl.sum("cogs")).alias("net_profit"),
                pl.mean("gross_amount").round(2).alias("avg_ticket"),
                pl.len().alias("total_orders"),
                pl.sum("discount_amount").alias("total_discount"),
            ]
        ).with_columns(
            [
                pl.lit(round(cancelled / total_count * 100, 2)).alias(
                    "cancellation_rate_pct"
                ),
                pl.lit(round(returned / total_count * 100, 2)).alias(
                    "return_rate_pct"
                ),
            ]
        )

    # ── Por vendedor ─────────────────────────────────────────────────────────
    if paid.is_empty():
        seller = pl.DataFrame(
            schema={
                "seller_id": pl.Utf8,
                "seller_name": pl.Utf8,
                "store_id": pl.Utf8,
                "store_name": pl.Utf8,
                "region": pl.Utf8,
                "gmv": pl.Float64,
                "net_revenue": pl.Float64,
                "cogs": pl.Float64,
                "gross_profit": pl.Float64,
                "gross_margin_pct": pl.Float64,
                "order_count": pl.Int64,
                "avg_ticket": pl.Float64,
                "avg_discount_pct": pl.Float64,
                "total_discount": pl.Float64,
                "target_gmv": pl.Float64,
                "target_orders": pl.Int64,
                "meta_achievement_pct": pl.Float64,
                "gmv_rank": pl.Int16,
            }
        )
    else:
        seller = (
            paid.group_by(
                ["seller_id", "seller_name", "store_id", "store_name", "region"]
            )
            .agg(
                [
                    pl.sum("gross_amount").alias("gmv"),
                    pl.sum("net_revenue").alias("net_revenue"),
                    pl.sum("cogs").alias("cogs"),
                    pl.len().alias("order_count"),
                    (pl.mean("discount_pct") * 100).round(2).alias("avg_discount_pct"),
                    pl.sum("discount_amount").alias("total_discount"),
                    pl.first("target_gmv").alias("target_gmv"),
                    pl.first("target_orders").alias("target_orders"),
                ]
            )
            .with_columns(
                [
                    (pl.col("net_revenue") - pl.col("cogs")).alias("gross_profit"),
                    (pl.col("gmv") / pl.col("order_count")).round(2).alias(
                        "avg_ticket"
                    ),
                ]
            )
            .with_columns(
                [
                    (
                        (pl.col("net_revenue") - pl.col("cogs"))
                        / pl.col("net_revenue").clip(lower_bound=0.01)
                        * 100
                    )
                    .round(2)
                    .alias("gross_margin_pct"),
                    (
                        pl.col("gmv")
                        / pl.col("target_gmv").clip(lower_bound=0.01)
                        * 100
                    )
                    .round(2)
                    .alias("meta_achievement_pct"),
                ]
            )
            .with_columns(
                pl.col("gmv")
                .rank(method="ordinal", descending=True)
                .cast(pl.Int16)
                .alias("gmv_rank")
            )
        )

    # ── Por categoria ────────────────────────────────────────────────────────
    if paid.is_empty():
        category = pl.DataFrame(
            schema={
                "category": pl.Utf8,
                "gmv": pl.Float64,
                "net_revenue": pl.Float64,
                "cogs": pl.Float64,
                "gross_profit": pl.Float64,
                "gross_margin_pct": pl.Float64,
                "order_count": pl.Int64,
                "avg_ticket": pl.Float64,
                "total_discount": pl.Float64,
                "cancellation_count": pl.Int64,
                "return_count": pl.Int64,
            }
        )
    else:
        cancelled_by_cat = (
            df.filter(pl.col("status") == "CANCELLED")
            .group_by("category")
            .agg(pl.len().alias("cancellation_count"))
        )
        returned_by_cat = (
            df.filter(pl.col("status") == "RETURNED")
            .group_by("category")
            .agg(pl.len().alias("return_count"))
        )

        category = (
            paid.group_by("category")
            .agg(
                [
                    pl.sum("gross_amount").alias("gmv"),
                    pl.sum("net_revenue").alias("net_revenue"),
                    pl.sum("cogs").alias("cogs"),
                    pl.len().alias("order_count"),
                    pl.mean("gross_amount").round(2).alias("avg_ticket"),
                    pl.sum("discount_amount").alias("total_discount"),
                ]
            )
            .with_columns(
                (pl.col("net_revenue") - pl.col("cogs")).alias("gross_profit")
            )
            .with_columns(
                (
                    (pl.col("net_revenue") - pl.col("cogs"))
                    / pl.col("net_revenue").clip(lower_bound=0.01)
                    * 100
                )
                .round(2)
                .alias("gross_margin_pct")
            )
            .join(cancelled_by_cat, on="category", how="left")
            .join(returned_by_cat, on="category", how="left")
            .with_columns(
                [
                    pl.col("cancellation_count").fill_null(0),
                    pl.col("return_count").fill_null(0),
                ]
            )
        )

    # ── Por canal ────────────────────────────────────────────────────────────
    if paid.is_empty():
        channel = pl.DataFrame(
            schema={
                "channel": pl.Utf8,
                "gmv": pl.Float64,
                "net_revenue": pl.Float64,
                "cogs": pl.Float64,
                "gross_profit": pl.Float64,
                "gross_margin_pct": pl.Float64,
                "order_count": pl.Int64,
                "avg_ticket": pl.Float64,
            }
        )
    else:
        channel = (
            paid.group_by("channel")
            .agg(
                [
                    pl.sum("gross_amount").alias("gmv"),
                    pl.sum("net_revenue").alias("net_revenue"),
                    pl.sum("cogs").alias("cogs"),
                    pl.len().alias("order_count"),
                    pl.mean("gross_amount").round(2).alias("avg_ticket"),
                ]
            )
            .with_columns(
                (pl.col("net_revenue") - pl.col("cogs")).alias("gross_profit")
            )
            .with_columns(
                (
                    (pl.col("net_revenue") - pl.col("cogs"))
                    / pl.col("net_revenue").clip(lower_bound=0.01)
                    * 100
                )
                .round(2)
                .alias("gross_margin_pct")
            )
        )

    # ── Por status ───────────────────────────────────────────────────────────
    status = (
        df.group_by("status")
        .agg(
            [
                pl.len().alias("order_count"),
                pl.sum("gross_amount").alias("gmv_impact"),
            ]
        )
        .with_columns(
            (pl.col("order_count") / total_count * 100)
            .round(2)
            .alias("pct_of_total")
        )
    )

    return {
        "clevel": clevel,
        "seller": seller,
        "category": category,
        "channel": channel,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Gravação no PostgreSQL — SQL = storage puro, sem lógica de negócio
# ---------------------------------------------------------------------------


def _insert_raw_events(
    conn: psycopg2.extensions.connection, batch: list[dict]
) -> None:
    """Grava eventos brutos. ON CONFLICT DO NOTHING garante idempotência."""
    now = datetime.now(UTC)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO raw_events (
                event_id, event_time, ingestion_time, order_id, order_date,
                customer_id, seller_id, seller_name, store_id, store_name, region,
                channel, category, product_id, product_name,
                quantity, unit_price, gross_amount, discount_pct, discount_amount,
                shipping_amount, net_revenue, cogs,
                status, payment_method, campaign,
                target_gmv, target_orders, processed_at
            ) VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            [
                (
                    r["event_id"],
                    r["event_time"],
                    r["ingestion_time"],
                    r["order_id"],
                    r["order_date"],
                    r.get("customer_id"),
                    r["seller_id"],
                    r["seller_name"],
                    r["store_id"],
                    r["store_name"],
                    r["region"],
                    r["channel"],
                    r["category"],
                    r["product_id"],
                    r["product_name"],
                    int(r["quantity"]),
                    float(r["unit_price"]),
                    float(r["gross_amount"]),
                    float(r["discount_pct"]),
                    float(r["discount_amount"]),
                    float(r["shipping_amount"]),
                    float(r["net_revenue"]),
                    float(r["cogs"]),
                    r["status"],
                    r["payment_method"],
                    r["campaign"],
                    float(r["target_gmv"]),
                    int(r["target_orders"]),
                    now,
                )
                for r in batch
            ],
            page_size=500,
        )


def _write_kpis(
    conn: psycopg2.extensions.connection, kpis: dict[str, pl.DataFrame]
) -> None:
    """
    Grava os KPIs pré-calculados pelo Polars. O SQL não faz nenhum cálculo:
    apenas substitui os valores existentes pelos novos (UPSERT por PK).
    """
    now = datetime.now(UTC)
    with conn.cursor() as cur:

        # kpi_clevel — sempre 1 linha
        row = kpis["clevel"].to_dicts()[0]
        cur.execute(
            """
            INSERT INTO kpi_clevel
                (id, gmv, net_profit, avg_ticket, total_orders,
                 total_discount, cancellation_rate_pct, return_rate_pct, updated_at)
            VALUES (1, %(gmv)s, %(net_profit)s, %(avg_ticket)s, %(total_orders)s,
                    %(total_discount)s, %(cancellation_rate_pct)s, %(return_rate_pct)s, %(now)s)
            ON CONFLICT (id) DO UPDATE SET
                gmv                   = EXCLUDED.gmv,
                net_profit            = EXCLUDED.net_profit,
                avg_ticket            = EXCLUDED.avg_ticket,
                total_orders          = EXCLUDED.total_orders,
                total_discount        = EXCLUDED.total_discount,
                cancellation_rate_pct = EXCLUDED.cancellation_rate_pct,
                return_rate_pct       = EXCLUDED.return_rate_pct,
                updated_at            = EXCLUDED.updated_at
            """,
            {**row, "now": now},
        )

        # kpi_seller
        if kpis["seller"].height > 0:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO kpi_seller
                    (seller_id, seller_name, store_id, store_name, region,
                     gmv, net_revenue, cogs, gross_profit, gross_margin_pct,
                     order_count, avg_ticket, avg_discount_pct, total_discount,
                     target_gmv, target_orders, meta_achievement_pct, gmv_rank, updated_at)
                VALUES %s
                ON CONFLICT (seller_id) DO UPDATE SET
                    gmv                  = EXCLUDED.gmv,
                    net_revenue          = EXCLUDED.net_revenue,
                    cogs                 = EXCLUDED.cogs,
                    gross_profit         = EXCLUDED.gross_profit,
                    gross_margin_pct     = EXCLUDED.gross_margin_pct,
                    order_count          = EXCLUDED.order_count,
                    avg_ticket           = EXCLUDED.avg_ticket,
                    avg_discount_pct     = EXCLUDED.avg_discount_pct,
                    total_discount       = EXCLUDED.total_discount,
                    meta_achievement_pct = EXCLUDED.meta_achievement_pct,
                    gmv_rank             = EXCLUDED.gmv_rank,
                    updated_at           = EXCLUDED.updated_at
                """,
                [
                    (
                        r["seller_id"], r["seller_name"], r["store_id"],
                        r["store_name"], r["region"],
                        float(r["gmv"]), float(r["net_revenue"]), float(r["cogs"]),
                        float(r["gross_profit"]), float(r["gross_margin_pct"]),
                        int(r["order_count"]), float(r["avg_ticket"]),
                        float(r["avg_discount_pct"]), float(r["total_discount"]),
                        float(r["target_gmv"]), int(r["target_orders"]),
                        float(r["meta_achievement_pct"]), int(r["gmv_rank"]), now,
                    )
                    for r in kpis["seller"].to_dicts()
                ],
            )

        # kpi_category
        if kpis["category"].height > 0:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO kpi_category
                    (category, gmv, net_revenue, cogs, gross_profit, gross_margin_pct,
                     order_count, avg_ticket, total_discount,
                     cancellation_count, return_count, updated_at)
                VALUES %s
                ON CONFLICT (category) DO UPDATE SET
                    gmv                = EXCLUDED.gmv,
                    net_revenue        = EXCLUDED.net_revenue,
                    cogs               = EXCLUDED.cogs,
                    gross_profit       = EXCLUDED.gross_profit,
                    gross_margin_pct   = EXCLUDED.gross_margin_pct,
                    order_count        = EXCLUDED.order_count,
                    avg_ticket         = EXCLUDED.avg_ticket,
                    total_discount     = EXCLUDED.total_discount,
                    cancellation_count = EXCLUDED.cancellation_count,
                    return_count       = EXCLUDED.return_count,
                    updated_at         = EXCLUDED.updated_at
                """,
                [
                    (
                        r["category"],
                        float(r["gmv"]), float(r["net_revenue"]), float(r["cogs"]),
                        float(r["gross_profit"]), float(r["gross_margin_pct"]),
                        int(r["order_count"]), float(r["avg_ticket"]),
                        float(r["total_discount"]),
                        int(r["cancellation_count"]), int(r["return_count"]), now,
                    )
                    for r in kpis["category"].to_dicts()
                ],
            )

        # kpi_channel
        if kpis["channel"].height > 0:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO kpi_channel
                    (channel, gmv, net_revenue, cogs, gross_profit, gross_margin_pct,
                     order_count, avg_ticket, updated_at)
                VALUES %s
                ON CONFLICT (channel) DO UPDATE SET
                    gmv              = EXCLUDED.gmv,
                    net_revenue      = EXCLUDED.net_revenue,
                    cogs             = EXCLUDED.cogs,
                    gross_profit     = EXCLUDED.gross_profit,
                    gross_margin_pct = EXCLUDED.gross_margin_pct,
                    order_count      = EXCLUDED.order_count,
                    avg_ticket       = EXCLUDED.avg_ticket,
                    updated_at       = EXCLUDED.updated_at
                """,
                [
                    (
                        r["channel"],
                        float(r["gmv"]), float(r["net_revenue"]), float(r["cogs"]),
                        float(r["gross_profit"]), float(r["gross_margin_pct"]),
                        int(r["order_count"]), float(r["avg_ticket"]), now,
                    )
                    for r in kpis["channel"].to_dicts()
                ],
            )

        # kpi_status
        if kpis["status"].height > 0:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO kpi_status (status, order_count, gmv_impact, pct_of_total, updated_at)
                VALUES %s
                ON CONFLICT (status) DO UPDATE SET
                    order_count  = EXCLUDED.order_count,
                    gmv_impact   = EXCLUDED.gmv_impact,
                    pct_of_total = EXCLUDED.pct_of_total,
                    updated_at   = EXCLUDED.updated_at
                """,
                [
                    (
                        r["status"], int(r["order_count"]),
                        float(r["gmv_impact"]), float(r["pct_of_total"]), now,
                    )
                    for r in kpis["status"].to_dicts()
                ],
            )


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------


def main() -> None:
    wait_for_kafka()
    init_schema()

    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {
            "bootstrap.servers": SETTINGS.kafka_bootstrap_servers,
            "group.id": "bf-processor-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.max.bytes": 8 * 1024 * 1024,
            "max.poll.interval.ms": 60_000,
        }
    )
    consumer.subscribe([SETTINGS.kafka_topic_orders])

    conn = _pg_connect()
    batch: list[dict] = []
    last_flush = time.monotonic()

    print("Stream processor iniciado.")

    while True:
        msg = consumer.poll(timeout=0.5)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Erro Kafka: {msg.error()}")
        else:
            batch.append(json.loads(msg.value()))

        elapsed = time.monotonic() - last_flush
        should_flush = len(batch) >= SETTINGS.processor_batch_size or (
            elapsed >= SETTINGS.processor_flush_seconds and len(batch) > 0
        )

        if should_flush:
            try:
                with conn:
                    _insert_raw_events(conn, batch)
                    raw_df = _load_raw_events(conn)
                    kpis = _compute_all_kpis(raw_df)
                    _write_kpis(conn, kpis)

                consumer.commit(asynchronous=False)

                clevel = kpis["clevel"].to_dicts()[0]
                print(
                    f"[flush] eventos={len(batch):>4} | "
                    f"raw_total={raw_df.height:>6} | "
                    f"GMV={clevel['gmv']:>12,.2f} | "
                    f"pedidos={clevel['total_orders']:>5} | "
                    f"elapsed={elapsed:.1f}s"
                )

            except Exception as exc:
                print(f"Erro no flush, reconectando: {exc}")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _pg_connect()

            batch.clear()
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
