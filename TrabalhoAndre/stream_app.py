import streamlit as st
import pandas as pd
from pathlib import Path
from streamlit_autorefresh import st_autorefresh

SILVER_PATH = Path("dados_silver")
ORDERS_PATH = SILVER_PATH / "orders_silver.csv"
SELLERS_PATH = SILVER_PATH / "sellers_silver.csv"

CHUNK_SIZE = 10
INTERVAL = 5

st.set_page_config(layout="wide")
st.title("🔥 Dashboard Black Friday - Real Time")

if "offset" not in st.session_state:
    st.session_state.offset = 0

if "dados" not in st.session_state:
    st.session_state.dados = pd.DataFrame()

df_sellers = pd.read_csv(SELLERS_PATH)

# =====================
# CARREGA NOVO CHUNK
# =====================

def carregar_chunk():
    df_chunk = pd.read_csv(
        ORDERS_PATH,
        skiprows=range(1, st.session_state.offset + 1),
        nrows=CHUNK_SIZE
    )

    if not df_chunk.empty:
        st.session_state.offset += CHUNK_SIZE
        df_chunk = df_chunk.merge(df_sellers, on="seller_id", how="left")
        st.session_state.dados = pd.concat(
            [st.session_state.dados, df_chunk],
            ignore_index=True
        )

carregar_chunk()

df = st.session_state.dados

# =====================
# C-Level
# =====================

st.header("🟣 C-LEVEL")

gmv = df["receita_liquida"].sum()
lucro_liquido = df["margem_valida"].sum()
ticket_medio = (
    df[df["is_cancelado"] == False]["receita_liquida"].mean()
    if len(df[df["is_cancelado"] == False]) > 0 else 0
)

col1, col2, col3 = st.columns(3)
col1.metric("GMV (Receita Bruta)", f"R$ {gmv:,.2f}")
col2.metric("Lucro Líquido", f"R$ {lucro_liquido:,.2f}")
col3.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}")

st.divider()

# =====================
# Camada de vendas
# =====================

st.header("🔵 VENDAS")

gmv_vendedor = (
    df.groupby("seller_id")["receita_valida"]
    .sum()
    .sort_values(ascending=False)
)

st.subheader("GMV por Vendedor")
st.bar_chart(gmv_vendedor)

num_pedidos = len(df)
st.metric("Número Total de Pedidos", num_pedidos)

desconto_medio = df["valor_desconto"].mean()
st.metric("Desconto Médio", f"R$ {desconto_medio:,.2f}")

top5 = gmv_vendedor.head(5)
bottom5 = gmv_vendedor.tail(5)
colA, colB = st.columns(2)
colA.subheader("Top 5 Vendedores")
colA.bar_chart(top5)
colB.subheader("Bottom 5 Vendedores")
colB.bar_chart(bottom5)

df_meta = df.merge(df_sellers[["seller_id", "meta_gmv"]], on="seller_id", how="left")
realizado = df_meta.groupby("seller_id")["receita_valida"].sum()
meta = df_sellers.set_index("seller_id")["meta_gmv"]
meta_vs_real = (realizado / meta * 100).fillna(0)
st.subheader("Meta vs Realizado (%)")
st.bar_chart(meta_vs_real)

st.divider()

# =====================
# Controladoria
# =====================

st.header("🟢 CONTROLADORIA")

margem_bruta = df["margem"].sum()
st.metric("Margem Bruta", f"R$ {margem_bruta:,.2f}")

custo_total = df["cogs"].sum()
st.metric("Custo Total", f"R$ {custo_total:,.2f}")

impacto_desconto = df["valor_desconto"].sum()
st.metric("Impacto Total Descontos", f"R$ {impacto_desconto:,.2f}")

cancelamentos = df["is_cancelado"].sum()
taxa_cancel = cancelamentos / len(df) * 100 if len(df) > 0 else 0
col1, col2 = st.columns(2)
col1.metric("Pedidos Cancelados", cancelamentos)
col2.metric("Taxa Cancelamento (%)", f"{taxa_cancel:.2f}%")

lucro_produto = (
    df.groupby("product_name")["margem_valida"]
    .sum()
    .sort_values(ascending=False)
)   
st.subheader("Lucro por Produto")
st.bar_chart(lucro_produto)

st.divider()

# =====================
# Outros Insights
# =====================

st.header("🟡 INSIGHTS COMPLEMENTARES")

receita_regiao = df.groupby("region")["receita_valida"].sum()
st.subheader("Receita por Região")
st.bar_chart(receita_regiao)

produto_cancel = df[df["is_cancelado"]].groupby("product_name").size()
st.subheader("Produto Mais Cancelado")
st.bar_chart(produto_cancel)

df_sorted = df.sort_values("event_time")
receita_acumulada = df_sorted["receita_valida"].cumsum()
st.subheader("Receita Acumulada")
st.line_chart(receita_acumulada)

# =====================
# AUTO REFRESH 
# =====================

st_autorefresh(interval=INTERVAL * 1000, key="auto_refresh")