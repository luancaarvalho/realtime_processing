import pandas as pd


def aggregate_metrics(df: pd.DataFrame) -> dict:
    paid_orders = df[df["is_paid"]]

    total_orders = df["order_id"].nunique()
    paid_orders_count = paid_orders["order_id"].nunique()

    gmv = df["gmv"].sum()
    revenue = df["valid_revenue"].sum()
    profit = df["profit"].sum()

    avg_ticket = revenue / paid_orders_count if paid_orders_count > 0 else 0.0
    total_discount = df["discount_amount"].sum()

    cancelled_orders = df[df["is_cancelled"]]["order_id"].nunique()
    returned_orders = df[df["is_returned"]]["order_id"].nunique()

    cancel_rate = cancelled_orders / total_orders if total_orders > 0 else 0.0
    return_rate = returned_orders / total_orders if total_orders > 0 else 0.0
    gross_margin = profit / revenue if revenue > 0 else 0.0
    conversion_rate = paid_orders_count / total_orders if total_orders > 0 else 0.0
    avg_discount = total_discount / total_orders if total_orders > 0 else 0.0

    return {
        "gmv": round(gmv, 2),
        "revenue": round(revenue, 2),
        "profit": round(profit, 2),
        "avg_ticket": round(avg_ticket, 2),
        "total_orders": int(total_orders),
        "paid_orders": int(paid_orders_count),
        "total_discount": round(total_discount, 2),
        "cancel_rate": round(cancel_rate, 4),
        "return_rate": round(return_rate, 4),
        "gross_margin": round(gross_margin, 4),
        "conversion_rate": round(conversion_rate, 4),
        "avg_discount": round(avg_discount, 2),
    }


def aggregate_by_seller(df: pd.DataFrame) -> list[dict]:
    seller_df = (
        df.groupby("seller_name", as_index=False)
        .agg(
            gmv=("gmv", "sum"),
            revenue=("valid_revenue", "sum"),
            orders=("order_id", "nunique"),
        )
        .sort_values("gmv", ascending=False)
    )

    return seller_df.to_dict(orient="records")


def aggregate_by_region(df: pd.DataFrame) -> list[dict]:
    region_df = (
        df.groupby("region", as_index=False)
        .agg(
            gmv=("gmv", "sum"),
            revenue=("valid_revenue", "sum"),
            orders=("order_id", "nunique"),
        )
        .sort_values("gmv", ascending=False)
    )

    return region_df.to_dict(orient="records")


def aggregate_top_products(df: pd.DataFrame) -> list[dict]:
    product_df = (
        df.groupby("product_name", as_index=False)
        .agg(
            quantity_sold=("quantity", "sum"),
            gmv=("gmv", "sum"),
            revenue=("valid_revenue", "sum"),
            orders=("order_id", "nunique"),
        )
        .sort_values(["quantity_sold", "gmv"], ascending=[False, False])
    )

    return product_df.to_dict(orient="records")