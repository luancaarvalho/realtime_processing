from __future__ import annotations

import time

import psycopg2

from settings import SETTINGS

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLES_SQL = """
-- Eventos brutos: fonte de verdade do pipeline.
-- ON CONFLICT (event_id) DO NOTHING garante idempotência em replays Kafka.
CREATE TABLE IF NOT EXISTS raw_events (
    event_id         TEXT        PRIMARY KEY,
    event_time       TIMESTAMPTZ NOT NULL,
    ingestion_time   TIMESTAMPTZ NOT NULL,
    order_id         TEXT        NOT NULL,
    order_date       DATE        NOT NULL,
    customer_id      TEXT,
    seller_id        TEXT        NOT NULL,
    seller_name      TEXT        NOT NULL,
    store_id         TEXT        NOT NULL,
    store_name       TEXT        NOT NULL,
    region           TEXT        NOT NULL,
    channel          TEXT        NOT NULL,
    category         TEXT        NOT NULL,
    product_id       TEXT        NOT NULL,
    product_name     TEXT        NOT NULL,
    quantity         SMALLINT    NOT NULL,
    unit_price       NUMERIC(12,2) NOT NULL,
    gross_amount     NUMERIC(12,2) NOT NULL,
    discount_pct     NUMERIC(8,6)  NOT NULL,
    discount_amount  NUMERIC(12,2) NOT NULL,
    shipping_amount  NUMERIC(12,2) NOT NULL,
    net_revenue      NUMERIC(12,2) NOT NULL,
    cogs             NUMERIC(12,2) NOT NULL,
    status           TEXT        NOT NULL,
    payment_method   TEXT        NOT NULL,
    campaign         TEXT        NOT NULL,
    target_gmv       NUMERIC(14,2),
    target_orders    INTEGER,
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_events_status   ON raw_events (status);
CREATE INDEX IF NOT EXISTS idx_raw_events_seller   ON raw_events (seller_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_evt_time ON raw_events (event_time DESC);

-- C-Level KPIs: uma única linha, substituída a cada flush.
-- Todas as colunas já calculadas pelo Polars — Metabase só exibe.
CREATE TABLE IF NOT EXISTS kpi_clevel (
    id                    SMALLINT    PRIMARY KEY DEFAULT 1,
    gmv                   NUMERIC(16,2) NOT NULL DEFAULT 0,
    net_profit            NUMERIC(16,2) NOT NULL DEFAULT 0,
    avg_ticket            NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_orders          INTEGER       NOT NULL DEFAULT 0,
    total_discount        NUMERIC(16,2) NOT NULL DEFAULT 0,
    cancellation_rate_pct NUMERIC(6,2)  NOT NULL DEFAULT 0,
    return_rate_pct       NUMERIC(6,2)  NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);
INSERT INTO kpi_clevel (id) VALUES (1) ON CONFLICT DO NOTHING;

-- KPIs por vendedor: uma linha por seller_id.
-- Inclui meta vs realizado calculado pelo Polars.
CREATE TABLE IF NOT EXISTS kpi_seller (
    seller_id            TEXT          PRIMARY KEY,
    seller_name          TEXT          NOT NULL,
    store_id             TEXT          NOT NULL,
    store_name           TEXT          NOT NULL,
    region               TEXT          NOT NULL,
    gmv                  NUMERIC(14,2) NOT NULL DEFAULT 0,
    net_revenue          NUMERIC(14,2) NOT NULL DEFAULT 0,
    cogs                 NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_profit         NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_margin_pct     NUMERIC(6,2)  NOT NULL DEFAULT 0,
    order_count          INTEGER       NOT NULL DEFAULT 0,
    avg_ticket           NUMERIC(12,2) NOT NULL DEFAULT 0,
    avg_discount_pct     NUMERIC(6,2)  NOT NULL DEFAULT 0,
    total_discount       NUMERIC(14,2) NOT NULL DEFAULT 0,
    target_gmv           NUMERIC(14,2) NOT NULL DEFAULT 0,
    target_orders        INTEGER       NOT NULL DEFAULT 0,
    meta_achievement_pct NUMERIC(6,2)  NOT NULL DEFAULT 0,
    gmv_rank             SMALLINT      NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- KPIs por categoria: uma linha por categoria.
CREATE TABLE IF NOT EXISTS kpi_category (
    category           TEXT          PRIMARY KEY,
    gmv                NUMERIC(14,2) NOT NULL DEFAULT 0,
    net_revenue        NUMERIC(14,2) NOT NULL DEFAULT 0,
    cogs               NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_profit       NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_margin_pct   NUMERIC(6,2)  NOT NULL DEFAULT 0,
    order_count        INTEGER       NOT NULL DEFAULT 0,
    avg_ticket         NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_discount     NUMERIC(14,2) NOT NULL DEFAULT 0,
    cancellation_count INTEGER       NOT NULL DEFAULT 0,
    return_count       INTEGER       NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- KPIs por canal: uma linha por canal.
CREATE TABLE IF NOT EXISTS kpi_channel (
    channel          TEXT          PRIMARY KEY,
    gmv              NUMERIC(14,2) NOT NULL DEFAULT 0,
    net_revenue      NUMERIC(14,2) NOT NULL DEFAULT 0,
    cogs             NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_profit     NUMERIC(14,2) NOT NULL DEFAULT 0,
    gross_margin_pct NUMERIC(6,2)  NOT NULL DEFAULT 0,
    order_count      INTEGER       NOT NULL DEFAULT 0,
    avg_ticket       NUMERIC(12,2) NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Resumo por status (PAID / CANCELLED / RETURNED).
CREATE TABLE IF NOT EXISTS kpi_status (
    status         TEXT          PRIMARY KEY,
    order_count    INTEGER       NOT NULL DEFAULT 0,
    gmv_impact     NUMERIC(14,2) NOT NULL DEFAULT 0,
    pct_of_total   NUMERIC(6,2)  NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_postgres(timeout_seconds: int = 120) -> psycopg2.extensions.connection:
    """Aguarda o PostgreSQL aceitar conexões e retorna a conexão aberta."""
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host=SETTINGS.postgres_host,
                port=SETTINGS.postgres_port,
                dbname=SETTINGS.postgres_db,
                user=SETTINGS.postgres_user,
                password=SETTINGS.postgres_password,
            )
            return conn
        except psycopg2.OperationalError:
            pass
        time.sleep(3)

    raise TimeoutError(
        f"PostgreSQL não ficou disponível em {timeout_seconds}s "
        f"({SETTINGS.postgres_host}:{SETTINGS.postgres_port}/{SETTINGS.postgres_db})"
    )


def init_schema() -> None:
    """Cria todas as tabelas KPI se ainda não existirem (idempotente)."""
    conn = wait_for_postgres()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLES_SQL)
        print("Schema inicializado com sucesso.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_schema()
