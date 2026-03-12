import pandas as pd


def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["is_paid"] = df["status"] == "PAID"
    df["is_cancelled"] = df["status"] == "CANCELLED"
    df["is_returned"] = df["status"] == "RETURNED"

    df["gmv"] = pd.to_numeric(df["gross_amount"], errors="coerce").fillna(0.0)
    df["net_revenue"] = pd.to_numeric(df["net_revenue"], errors="coerce").fillna(0.0)
    df["cogs"] = pd.to_numeric(df["cogs"], errors="coerce").fillna(0.0)
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0.0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    df["valid_revenue"] = df["net_revenue"].where(df["is_paid"], 0.0)
    df["valid_cogs"] = df["cogs"].where(df["is_paid"], 0.0)
    df["profit"] = df["valid_revenue"] - df["valid_cogs"]

    df["cancelled_value"] = df["net_revenue"].where(df["is_cancelled"], 0.0)
    df["returned_value"] = df["net_revenue"].where(df["is_returned"], 0.0)

    return df