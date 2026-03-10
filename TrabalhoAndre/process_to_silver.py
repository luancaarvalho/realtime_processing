import pandas as pd
from pathlib import Path

RAW_PATH = Path("dados_raw")
SILVER_PATH = Path("dados_silver")

ORDERS_RAW = RAW_PATH / "orders_seed.csv"
SELLERS_RAW = RAW_PATH / "sellers_targets.csv"

ORDERS_SILVER = SILVER_PATH / "orders_silver.csv"
SELLERS_SILVER = SILVER_PATH / "sellers_silver.csv"


def main():

    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    print("\n========== PROCESSANDO ORDERS ==========\n")

    df_orders = pd.read_csv(ORDERS_RAW)
    print(f"Linhas originais: {len(df_orders)}")

    df_orders["event_time"] = pd.to_datetime(df_orders["event_time"])
    df_orders["ingestion_time"] = pd.to_datetime(df_orders["ingestion_time"])

    # Flag cancelamento
    df_orders["is_cancelado"] = df_orders["status"] == "CANCELLED"

    # Enriquecimento
    df_orders["margem"] = df_orders["net_revenue"] - df_orders["cogs"]
    df_orders["ticket_medio"] = df_orders["net_revenue"] / df_orders["quantity"]

    # Receita válida (zera cancelado)
    df_orders["receita_valida"] = df_orders.apply(
        lambda x: 0 if x["is_cancelado"] else x["net_revenue"], axis=1
    )

    df_orders["margem_valida"] = df_orders.apply(
        lambda x: 0 if x["is_cancelado"] else x["margem"], axis=1
    )

    novas_colunas = [
        "is_cancelado",
        "margem",
        "ticket_medio",
        "receita_valida",
        "margem_valida"
    ]

    df_orders = df_orders.rename(columns={
        "net_revenue": "receita_liquida",
        "gross_amount": "valor_bruto",
        "discount_amount": "valor_desconto",
        "shipping_amount": "valor_frete"
    })

    df_orders.to_csv(ORDERS_SILVER, index=False)

    print(f"\nTotal final de linhas: {len(df_orders)}")
    print(f"Novas colunas criadas: {novas_colunas}")
    print(f"Total colunas finais: {len(df_orders.columns)}")
    print(df_orders.head())

    print("\n========== PROCESSANDO SELLERS ==========\n")

    df_sellers = pd.read_csv(SELLERS_RAW)

    df_sellers = df_sellers.rename(columns={
        "target_gmv": "meta_gmv",
        "target_orders": "meta_pedidos"
    })

    df_sellers.to_csv(SELLERS_SILVER, index=False)

    print(f"Total sellers: {len(df_sellers)}")
    print(df_sellers.head())

    print("\n✅ Camada SILVER criada com sucesso!\n")


if __name__ == "__main__":
    main()