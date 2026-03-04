from __future__ import annotations

import time

import pandas as pd
import plotly.express as px
import streamlit as st

from minio_utils import build_s3_client, get_json
from settings import SETTINGS


st.set_page_config(page_title="Black Friday Dashboard", layout="wide")


@st.cache_resource
def s3_client():
    return build_s3_client()


def currency(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def render_empty() -> None:
    st.title("Black Friday Dashboard")
    st.info("Aguardando metricas no MinIO. Verifique se bf-processor esta rodando.")


def render_dashboard(payload: dict) -> None:
    c_level = payload.get("c_level", {})
    vendas = payload.get("vendas", {})
    controladoria = payload.get("controladoria", {})

    st.title("Black Friday Dashboard")
    st.caption(f"Atualizado em: {payload.get('updated_at', 'n/a')} | Auto refresh a cada 5s")

    st.header("C-Level")
    c1, c2, c3 = st.columns(3)
    c1.metric("GMV", currency(float(c_level.get("gmv", 0.0))))
    c2.metric("Lucro Liquido", currency(float(c_level.get("lucro_liquido", 0.0))))
    c3.metric("Ticket Medio", currency(float(c_level.get("ticket_medio", 0.0))))

    st.header("Vendas")
    v1, v2, v3 = st.columns(3)
    v1.metric("Numero de pedidos", int(vendas.get("numero_pedidos", 0)))
    v2.metric("Ticket medio", currency(float(vendas.get("ticket_medio", 0.0))))
    v3.metric("Desconto medio", currency(float(vendas.get("desconto_medio", 0.0))))

    sellers = pd.DataFrame(vendas.get("gmv_por_vendedor", []))
    stores = pd.DataFrame(vendas.get("gmv_por_loja", []))
    meta_df = pd.DataFrame(vendas.get("meta_vs_realizado", []))

    if not sellers.empty:
        sellers_plot = sellers.sort_values("gmv", ascending=False).head(10)
        fig_sellers = px.bar(sellers_plot, x="seller_name", y="gmv", title="GMV por vendedor (Top 10)")
        st.plotly_chart(fig_sellers, use_container_width=True)

    if not stores.empty:
        fig_stores = px.bar(stores.sort_values("gmv", ascending=False), x="store_name", y="gmv", title="GMV por loja")
        st.plotly_chart(fig_stores, use_container_width=True)

    if not meta_df.empty:
        fig_meta = px.bar(
            meta_df.sort_values("atingimento_pct", ascending=False),
            x="seller_name",
            y="atingimento_pct",
            title="Meta vs realizado (% atingimento)",
        )
        st.plotly_chart(fig_meta, use_container_width=True)

    col_top, col_bottom = st.columns(2)
    col_top.subheader("Top performers")
    col_top.dataframe(pd.DataFrame(vendas.get("top_performers", [])), use_container_width=True)

    col_bottom.subheader("Bottom performers")
    col_bottom.dataframe(pd.DataFrame(vendas.get("bottom_performers", [])), use_container_width=True)

    st.header("Controladoria")
    ct1, ct2, ct3, ct4 = st.columns(4)
    ct1.metric("Margem bruta", pct(float(controladoria.get("margem_bruta", 0.0))))
    ct2.metric("Custo", currency(float(controladoria.get("custo", 0.0))))
    ct3.metric("Impacto descontos", currency(float(controladoria.get("impacto_descontos", 0.0))))
    ct4.metric(
        "Cancelamentos / Devolucoes",
        f"{int(controladoria.get('cancelamentos', 0))} / {int(controladoria.get('devolucoes', 0))}",
    )

    lucro_categoria = pd.DataFrame(controladoria.get("lucro_por_categoria", []))
    lucro_canal = pd.DataFrame(controladoria.get("lucro_por_canal", []))

    if not lucro_categoria.empty:
        fig_cat = px.bar(
            lucro_categoria.sort_values("profit", ascending=False),
            x="category",
            y="profit",
            title="Lucro por categoria",
        )
        st.plotly_chart(fig_cat, use_container_width=True)

    if not lucro_canal.empty:
        fig_channel = px.bar(
            lucro_canal.sort_values("profit", ascending=False),
            x="channel",
            y="profit",
            title="Lucro por canal",
        )
        st.plotly_chart(fig_channel, use_container_width=True)


st.markdown("<meta http-equiv='refresh' content='5'>", unsafe_allow_html=True)
time.sleep(0.1)

payload = get_json(s3_client(), SETTINGS.dashboard_metrics_key)
if not payload:
    render_empty()
else:
    render_dashboard(payload)
