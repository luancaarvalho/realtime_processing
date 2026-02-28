import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

st.set_page_config(page_title="Black Friday Real-Time Monitor", layout="wide")

st.markdown(
    """
    <style>
    .main { background-color: #0e1117; }
    h1, h2, h3 { color: #ff4b4b; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🚀 Monitor Black Friday - Dashboards de Vendas")

DB_PATH = "/app/data/blackfriday.db"

# Auto-refresh control: sidebar slider (0 = off). Default = 15s for automatic updates
refresh_seconds = st.sidebar.slider("Auto-refresh (seconds, 0 = off)", 0, 60, 15)
if refresh_seconds > 0:
    st.markdown(
        f'<meta http-equiv="refresh" content="{refresh_seconds}">',
        unsafe_allow_html=True,
    )
    st.sidebar.write(f"Atualizando a cada {refresh_seconds}s")
    st.sidebar.write(
        "Última atualização: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


def get_data():
    if not os.path.exists(DB_PATH):
        return None
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute("SELECT * FROM sales").df()
        con.close()
        return df
    except Exception as e:
        return None


df = get_data()

if df is None or df.empty:
    st.warning("⏳ Aguardando processamento dos primeiros dados do Kafka...")
    if st.button("Tentar Novamente"):
        st.rerun()
    st.stop()

df["profit"] = df["net_revenue"] - df["cogs"]
total_gmv = df["gross_amount"].sum()
total_profit = df["profit"].sum()
avg_ticket = df["gross_amount"].mean()
total_orders = len(df)
avg_discount = (
    (df["discount_amount"].sum() / df["gross_amount"].sum()) * 100
    if total_gmv > 0
    else 0
)
churn_rate = (
    len(df[df["status"].isin(["CANCELLED", "RETURNED"])]) / total_orders
) * 100

st.header("📊 1. Visão Executiva (C-Level)")
c1, c2, c3, c4 = st.columns(4)
c1.metric("GMV Bruto Total", f"R$ {total_gmv:,.2f}")
c2.metric("Lucro Líquido", f"R$ {total_profit:,.2f}")
c3.metric("Ticket Médio", f"R$ {avg_ticket:,.2f}")
c4.metric("Total de Pedidos", f"{total_orders:,}")

st.divider()
st.header("🛍️ 2. Performance de Vendas")
col_v1, col_v2 = st.columns([2, 1])

with col_v1:
    df_seller = (
        df.groupby("seller_name")["gross_amount"]
        .sum()
        .reset_index()
        .sort_values("gross_amount", ascending=False)
    )
    fig_seller = px.bar(
        df_seller.head(10),
        x="gross_amount",
        y="seller_name",
        orientation="h",
        title="Top 10 Vendedores (GMV)",
        color="gross_amount",
        color_continuous_scale="Reds",
    )
    st.plotly_chart(fig_seller, use_container_width=True)

with col_v2:
    meta_global = df["target_gmv"].unique().sum()
    fig_gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=total_gmv,
            title={"text": "Meta Global de GMV (R$)"},
            gauge={
                "axis": {"range": [None, meta_global * 1.1]},
                "bar": {"color": "#ff4b4b"},
            },
        )
    )
    st.plotly_chart(fig_gauge, use_container_width=True)

st.divider()
st.header("⚖️ 3. Controladoria e Margem")
ct1, ct2, ct3 = st.columns(3)

with ct1:
    st.metric("Desconto Médio (Mix)", f"{avg_discount:.2f}%")
    st.metric("Taxa de Cancelamento/Devol.", f"{churn_rate:.2f}%")

with ct2:
    df_profit_cat = df.groupby("category")["profit"].sum().reset_index()
    st.plotly_chart(
        px.bar(df_profit_cat, x="category", y="profit", title="Lucro por Categoria"),
        use_container_width=True,
    )

with ct3:
    df_profit_channel = df.groupby("channel")["profit"].sum().reset_index()
    st.plotly_chart(
        px.pie(
            df_profit_channel, values="profit", names="channel", title="Lucro por Canal"
        ),
        use_container_width=True,
    )

if st.sidebar.button("🔄 Atualizar Dados"):
    st.rerun()
