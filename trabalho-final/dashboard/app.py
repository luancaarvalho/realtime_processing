import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

METRICS_FILE = Path("trabalho-final/data/metrics.json")

st.set_page_config(
    page_title="Dashboard Black Friday",
    layout="wide",
)

st.markdown("""
    <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .main-title {
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }
        .subtitle {
            color: #6b7280;
            margin-bottom: 1.2rem;
        }
        .section-title {
            font-size: 1.35rem;
            font-weight: 700;
            margin-top: 1rem;
            margin-bottom: 0.8rem;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-title">📊 Dashboard Black Friday</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Indicadores C-Level, Performance Comercial e Controladoria</div>',
    unsafe_allow_html=True
)

top_col1, top_col2 = st.columns([1, 4])

with top_col1:
    if st.button("🔄 Atualizar dados", use_container_width=True):
        st.rerun()

if not METRICS_FILE.exists():
    st.warning("Arquivo de métricas ainda não encontrado. Rode o consumer primeiro.")
    st.stop()

try:
    with open(METRICS_FILE, "r", encoding="utf-8") as f:
        metrics = json.load(f)
except json.JSONDecodeError:
    st.warning("Arquivo de métricas ainda está sendo atualizado. Clique em atualizar novamente.")
    st.stop()

last_update = metrics.get("last_update", "Não disponível")
st.caption(f"Última atualização: {last_update}")

gmv = float(metrics["gmv"])
revenue = float(metrics["revenue"])
profit = float(metrics["profit"])
avg_ticket = float(metrics["avg_ticket"])
total_orders = int(metrics["total_orders"])
paid_orders = int(metrics["paid_orders"])
total_discount = float(metrics["total_discount"])
cancel_rate = float(metrics["cancel_rate"])
return_rate = float(metrics["return_rate"])
gross_margin = float(metrics["gross_margin"])
conversion_rate = float(metrics["conversion_rate"])
avg_discount = float(metrics["avg_discount"])

cancelled_orders = round(total_orders * cancel_rate)
returned_orders = round(total_orders * return_rate)

if cancelled_orders + returned_orders + paid_orders > total_orders:
    overflow = (cancelled_orders + returned_orders + paid_orders) - total_orders
    returned_orders = max(returned_orders - overflow, 0)

# ---------- helpers ----------
def make_horizontal_bar(df: pd.DataFrame, category_col: str, value_col: str, title: str, height: int = 320):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_col}:Q", title=""),
            y=alt.Y(
                f"{category_col}:N",
                sort=alt.SortField(field=value_col, order="descending"),
                title="",
            ),
            tooltip=[category_col, value_col],
        )
        .properties(title=title, height=height)
    )
    return chart

# ---------- C-LEVEL ----------
st.markdown("---")
st.markdown('<div class="section-title">C-Level</div>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)
col1.metric("GMV", f"R$ {gmv:,.2f}")
col2.metric("Receita", f"R$ {revenue:,.2f}")
col3.metric("Lucro", f"R$ {profit:,.2f}")
col4.metric("Ticket Médio", f"R$ {avg_ticket:,.2f}")

st.markdown("")

summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
summary_col1.metric("Pedidos Totais", f"{total_orders}")
summary_col2.metric("Pedidos Pagos", f"{paid_orders}")
summary_col3.metric("Conversão", f"{conversion_rate * 100:.2f}%")
summary_col4.metric("Desconto Médio", f"R$ {avg_discount:,.2f}")

# ---------- PERFORMANCE COMERCIAL ----------
st.markdown("---")
st.markdown('<div class="section-title">Performance Comercial</div>', unsafe_allow_html=True)

perf_col1, perf_col2 = st.columns(2)

with perf_col1:
    st.markdown("**Top vendedores por GMV**")
    seller_data = metrics.get("by_seller", [])

    if seller_data:
        seller_df = pd.DataFrame(seller_data)
        seller_df["gmv"] = pd.to_numeric(seller_df["gmv"], errors="coerce")
        seller_df = seller_df.dropna(subset=["gmv"])
        seller_df = seller_df.sort_values("gmv", ascending=False).head(10)

        if not seller_df.empty:
            st.altair_chart(
                make_horizontal_bar(
                    seller_df,
                    category_col="seller_name",
                    value_col="gmv",
                    title="Top vendedores por GMV",
                ),
                use_container_width=True,
            )
        else:
            st.info("Sem dados de vendedores para exibir.")
    else:
        st.info("Ainda não há dados de vendedores.")

with perf_col2:
    st.markdown("**GMV por região**")
    region_data = metrics.get("by_region", [])

    if region_data:
        region_df = pd.DataFrame(region_data)
        region_df["gmv"] = pd.to_numeric(region_df["gmv"], errors="coerce")
        region_df = region_df.dropna(subset=["gmv"])
        region_df = region_df.sort_values("gmv", ascending=False)

        if not region_df.empty:
            st.altair_chart(
                make_horizontal_bar(
                    region_df,
                    category_col="region",
                    value_col="gmv",
                    title="GMV por região",
                ),
                use_container_width=True,
            )
        else:
            st.info("Sem dados de região para exibir.")
    else:
        st.info("Ainda não há dados por região.")

st.markdown("")

prod_col1, prod_col2 = st.columns(2)

with prod_col1:
    st.markdown("**Top produtos por quantidade vendida**")
    product_data = metrics.get("top_products", [])

    if product_data:
        product_df = pd.DataFrame(product_data)
        product_df["quantity_sold"] = pd.to_numeric(product_df["quantity_sold"], errors="coerce")
        product_df = product_df.dropna(subset=["quantity_sold"])
        product_df = product_df.sort_values("quantity_sold", ascending=False).head(10)

        if not product_df.empty:
            st.altair_chart(
                make_horizontal_bar(
                    product_df,
                    category_col="product_name",
                    value_col="quantity_sold",
                    title="Top produtos por quantidade vendida",
                ),
                use_container_width=True,
            )
        else:
            st.info("Sem dados de produtos para exibir.")
    else:
        st.info("Ainda não há dados de produtos.")

with prod_col2:
    st.markdown("**Top produtos por GMV**")
    product_data = metrics.get("top_products", [])

    if product_data:
        product_df = pd.DataFrame(product_data)
        product_df["gmv"] = pd.to_numeric(product_df["gmv"], errors="coerce")
        product_df = product_df.dropna(subset=["gmv"])
        product_df = product_df.sort_values("gmv", ascending=False).head(10)

        if not product_df.empty:
            st.altair_chart(
                make_horizontal_bar(
                    product_df,
                    category_col="product_name",
                    value_col="gmv",
                    title="Top produtos por GMV",
                ),
                use_container_width=True,
            )
        else:
            st.info("Sem dados de produtos para exibir.")
    else:
        st.info("Ainda não há dados de produtos.")

# ---------- CONTROLADORIA ----------
st.markdown("---")
st.markdown('<div class="section-title">Controladoria</div>', unsafe_allow_html=True)

ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4 = st.columns(4)
ctrl_col1.metric("Taxa de Cancelamento", f"{cancel_rate * 100:.2f}%")
ctrl_col2.metric("Taxa de Devolução", f"{return_rate * 100:.2f}%")
ctrl_col3.metric("Desconto Total", f"R$ {total_discount:,.2f}")
ctrl_col4.metric("Margem Bruta", f"{gross_margin * 100:.2f}%")

st.markdown("")

st.markdown("**Funil de pedidos**")

funnel_df = pd.DataFrame(
    {
        "Etapa": ["Pedidos Totais", "Pedidos Pagos", "Cancelados", "Devolvidos"],
        "Quantidade": [total_orders, paid_orders, cancelled_orders, returned_orders],
    }
)

funnel_df["Quantidade"] = pd.to_numeric(funnel_df["Quantidade"], errors="coerce")
funnel_df = funnel_df.sort_values("Quantidade", ascending=False)

funnel_chart = (
    alt.Chart(funnel_df)
    .mark_bar()
    .encode(
        x=alt.X("Quantidade:Q", title=""),
        y=alt.Y(
            "Etapa:N",
            sort=alt.SortField(field="Quantidade", order="descending"),
            title="",
        ),
        tooltip=["Etapa", "Quantidade"],
    )
    .properties(title="Funil de pedidos", height=260)
)

st.altair_chart(funnel_chart, use_container_width=True)