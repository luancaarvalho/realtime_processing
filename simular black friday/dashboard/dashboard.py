import streamlit as st
import duckdb
import polars as pl
from streamlit_autorefresh import st_autorefresh

# atualização 5s
st_autorefresh(interval=5000, key="refresh")

st.set_page_config(
    page_title="Black Friday Analytics",
    layout="wide"
)

st.title("🛒 Black Friday - Real Time Dashboard")

# conectar com duckdb 
DB_PATH = r"C:\Users\Maria\realtime_processing_mariana\simular black friday\streaming\database\black_friday.duckdb"
with duckdb.connect(DB_PATH, read_only=True) as con:
    df = con.execute("SELECT * FROM sales").pl()

if df.height == 0:
    st.warning("Nenhum dado recebido ainda.")
    st.stop()

# os indicadores c-LEVEL

st.header("C-Level")

gmv = df["gross_value"].sum()
profit = df["gross_profit"].sum()
ticket = df["net_revenue"].mean()

col1, col2, col3 = st.columns(3)

col1.metric("GMV", f"R$ {gmv:,.2f}")
col2.metric("Lucro Líquido", f"R$ {profit:,.2f}")
col3.metric("Ticket Médio", f"R$ {ticket:,.2f}")

#os indicadores de vendas
st.header("Vendas")

seller_gmv = (
    df.group_by("seller_id")
    .agg(pl.col("gross_value").sum().alias("gmv"))
    .sort("gmv", descending=True)
)

store_gmv = (
    df.group_by("store_id")
    .agg(pl.col("gross_value").sum().alias("gmv"))
    .sort("gmv", descending=True)
)

orders = df.height
avg_discount = df["discount_value"].mean()

col1, col2, col3 = st.columns(3)

col1.metric("Número de pedidos", orders)
col2.metric("Ticket médio", f"R$ {ticket:,.2f}")
col3.metric("Desconto médio", f"R$ {avg_discount:,.2f}")

col1, col2 = st.columns(2)

with col1:
    st.subheader("GMV por vendedor")
    st.dataframe(seller_gmv)

with col2:
    st.subheader("GMV por loja")
    st.dataframe(store_gmv)

top_performers = seller_gmv.head(5)
bottom_performers = seller_gmv.tail(5)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top performers")
    st.dataframe(top_performers)

with col2:
    st.subheader("Bottom performers")
    st.dataframe(bottom_performers)

# indicadores de controladoria 

st.header("Controladoria")

net_revenue = df["net_revenue"].sum()
cost = df["cost"].sum()
discount_total = df["discount_value"].sum()

margin = profit / net_revenue if net_revenue > 0 else 0

cancelled = df.filter(pl.col("is_cancelled") == True).height
returns = df.filter(pl.col("is_returned") == True).height

col1, col2, col3 = st.columns(3)

col1.metric("Margem bruta", f"{margin:.2%}")
col2.metric("Custo total", f"R$ {cost:,.2f}")
col3.metric("Impacto descontos", f"R$ {discount_total:,.2f}")

col1, col2 = st.columns(2)

col1.metric("Cancelamentos", cancelled)
col2.metric("Devoluções", returns)

profit_category = (
    df.group_by("category")
    .agg(pl.col("profit").sum())
    .sort("profit", descending=True)
)

st.subheader("Lucro por categoria")
st.bar_chart(
    profit_category.to_pandas().set_index("category")
)

profit_channel = (
    df.group_by("channel")
    .agg(pl.col("profit").sum())
)

st.subheader("Lucro por canal")
st.bar_chart(
    profit_channel.to_pandas().set_index("channel")
)