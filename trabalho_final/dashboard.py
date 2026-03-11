"""
dashboard.py — Dashboard Black Friday (Streamlit)
Seções: C-Level | Vendas | Controladoria
Auto-refresh a cada 5 segundos via streamlit-autorefresh.
"""
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Black Friday Dashboard",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

import os
DB_PATH  = os.environ.get("DB_PATH", "blackfriday.db")
TAX_RATE = 0.27   # 27% de impostos simulados

# ── Auto-refresh: 5 segundos ──────────────────────────────────────────────────
st_autorefresh(interval=5_000, key="bf_refresh")

# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def query(sql: str, params=()) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(sql, conn, params=params)

def fmt_brl(v: float) -> str:
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_pct(v: float) -> str:
    return f"{v:.1f}%"

def card(col, label: str, value: str, delta: str = ""):
    col.metric(label, value, delta)

CORES = px.colors.qualitative.Bold

# ── SQL helpers ───────────────────────────────────────────────────────────────
SQL_CONCLUIDO = "WHERE v.status = 'Concluído'"

def df_concluido() -> pd.DataFrame:
    return query(f"""
        SELECT v.*, p.categoria, p.custo_unitario, ve.nome AS vendedor, ve.meta_gmv,
               l.nome AS loja, l.regiao, l.tipo
        FROM vendas v
        JOIN produtos   p  ON v.produto_id   = p.id
        JOIN vendedores ve ON v.vendedor_id   = ve.id
        JOIN lojas      l  ON v.loja_id       = l.id
        {SQL_CONCLUIDO}
    """)

def df_todas() -> pd.DataFrame:
    return query("""
        SELECT v.status, v.valor_liquido, v.custo_total, v.desconto_pct,
               v.quantidade, v.timestamp, p.categoria, l.nome AS loja
        FROM vendas v
        JOIN produtos p ON v.produto_id = p.id
        JOIN lojas    l ON v.loja_id    = l.id
    """)

# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown(
    """
    <h1 style='text-align:center;color:#FF4B4B;margin-bottom:0'>
        🛍️ Black Friday — Dashboard de Vendas
    </h1>
    <p style='text-align:center;color:#888;margin-top:4px'>
        Atualização automática a cada 5 segundos
    </p>
    """,
    unsafe_allow_html=True,
)
st.divider()

# ── Carrega dados ─────────────────────────────────────────────────────────────
df  = df_concluido()
dft = df_todas()

if df.empty:
    st.warning("Nenhuma venda registrada ainda. Inicie o generator.py.")
    st.stop()

# Campos derivados
df["lucro_bruto"]  = (df["valor_liquido"] - df["custo_total"]) * (1 - TAX_RATE)
df["timestamp"]    = pd.to_datetime(df["timestamp"])
df["hora"]         = df["timestamp"].dt.hour

dft["timestamp"]   = pd.to_datetime(dft["timestamp"])
dft["hora"]        = dft["timestamp"].dt.hour

# KPIs globais
gmv          = df["valor_liquido"].sum()
lucro_liq    = df["lucro_bruto"].sum()
n_pedidos    = df["pedido_id"].nunique()
ticket_medio = gmv / max(n_pedidos, 1)
desc_medio   = df["desconto_pct"].mean()

total_todas  = len(dft)
total_cancel = (dft["status"] == "Cancelado").sum()
total_dev    = (dft["status"] == "Devolvido").sum()
custo_total  = df["custo_total"].sum()
margem_bruta = (gmv - custo_total) / gmv * 100 if gmv > 0 else 0
impacto_desc = dft.apply(
    lambda r: r["valor_liquido"] / (1 - r["desconto_pct"] / 100) * r["desconto_pct"] / 100
    if r["desconto_pct"] < 100 else 0, axis=1
).sum()

# ══════════════════════════════════════════════════════════════════════════════
# ABAS
# ══════════════════════════════════════════════════════════════════════════════
aba1, aba2, aba3 = st.tabs(["📊 C-Level", "🏪 Vendas", "📋 Controladoria"])

# ╔══════════════════════════════════════════════════════════════════════════════
# ABA 1 — C-LEVEL
# ╚══════════════════════════════════════════════════════════════════════════════
with aba1:
    st.subheader("Visão Executiva")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 GMV",          fmt_brl(gmv))
    c2.metric("📈 Lucro Líquido", fmt_brl(lucro_liq))
    c3.metric("🎟️ Ticket Médio",  fmt_brl(ticket_medio))
    c4.metric("📦 Pedidos",       f"{n_pedidos:,}")

    st.divider()
    col_a, col_b = st.columns([2, 1])

    with col_a:
        # GMV acumulado por hora
        gmv_hora = (
            df.groupby("hora")["valor_liquido"]
            .sum()
            .reindex(range(24), fill_value=0)
            .reset_index()
        )
        gmv_hora.columns = ["Hora", "GMV"]
        gmv_hora["GMV_acum"] = gmv_hora["GMV"].cumsum()

        fig = go.Figure()
        fig.add_bar(x=gmv_hora["Hora"], y=gmv_hora["GMV"],
                    name="GMV por Hora", marker_color="#FF4B4B", opacity=0.7)
        fig.add_scatter(x=gmv_hora["Hora"], y=gmv_hora["GMV_acum"],
                        name="GMV Acumulado", line=dict(color="#FFA500", width=3),
                        yaxis="y2")
        fig.update_layout(
            title="GMV por Hora do Dia",
            yaxis=dict(title="GMV"),
            yaxis2=dict(title="Acumulado", overlaying="y", side="right"),
            legend=dict(orientation="h"),
            template="plotly_dark",
            height=360,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        # GMV por canal
        gmv_canal = df.groupby("canal")["valor_liquido"].sum().reset_index()
        gmv_canal.columns = ["Canal", "GMV"]
        fig2 = px.pie(gmv_canal, values="GMV", names="Canal",
                      title="GMV por Canal",
                      color_discrete_sequence=CORES,
                      hole=0.45)
        fig2.update_layout(template="plotly_dark", height=360)
        st.plotly_chart(fig2, use_container_width=True)

    # GMV por categoria
    gmv_cat = df.groupby("categoria")["valor_liquido"].sum().sort_values(ascending=True).reset_index()
    gmv_cat.columns = ["Categoria", "GMV"]
    fig3 = px.bar(gmv_cat, x="GMV", y="Categoria", orientation="h",
                  title="GMV por Categoria", color="Categoria",
                  color_discrete_sequence=CORES, text_auto=".2s")
    fig3.update_layout(template="plotly_dark", showlegend=False, height=320)
    st.plotly_chart(fig3, use_container_width=True)

# ╔══════════════════════════════════════════════════════════════════════════════
# ABA 2 — VENDAS
# ╚══════════════════════════════════════════════════════════════════════════════
with aba2:
    st.subheader("Performance de Vendas")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Nº de Pedidos",    f"{n_pedidos:,}")
    c2.metric("🎟️ Ticket Médio",     fmt_brl(ticket_medio))
    c3.metric("🏷️ Desconto Médio",   fmt_pct(desc_medio))
    c4.metric("🛒 Itens Vendidos",   f"{int(df['quantidade'].sum()):,}")

    st.divider()

    # ── GMV por Vendedor ──────────────────────────────────────────────────────
    gmv_vend = (
        df.groupby(["vendedor_id", "vendedor", "meta_gmv"])["valor_liquido"]
        .sum()
        .reset_index()
        .rename(columns={"valor_liquido": "GMV"})
        .sort_values("GMV", ascending=False)
    )
    gmv_vend["Meta"]      = gmv_vend["meta_gmv"]
    gmv_vend["Ating_%"]   = (gmv_vend["GMV"] / gmv_vend["Meta"] * 100).round(1)
    gmv_vend["Cor"]       = gmv_vend["Ating_%"].apply(
        lambda x: "#2ecc71" if x >= 100 else ("#f39c12" if x >= 70 else "#e74c3c")
    )

    col_v1, col_v2 = st.columns(2)

    with col_v1:
        top10 = gmv_vend.head(10)
        fig = px.bar(top10, x="GMV", y="vendedor", orientation="h",
                     title="Top 10 Vendedores — GMV", color="Cor",
                     color_discrete_map="identity", text_auto=".2s")
        fig.update_layout(template="plotly_dark", showlegend=False,
                          yaxis=dict(autorange="reversed"), height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col_v2:
        bot10 = gmv_vend.tail(10).sort_values("GMV")
        fig2 = px.bar(bot10, x="GMV", y="vendedor", orientation="h",
                      title="Bottom 10 Vendedores — GMV", color="Cor",
                      color_discrete_map="identity", text_auto=".2s")
        fig2.update_layout(template="plotly_dark", showlegend=False,
                           yaxis=dict(autorange="reversed"), height=380)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ── GMV por Loja + Meta vs Realizado ─────────────────────────────────────
    col_l1, col_l2 = st.columns(2)

    with col_l1:
        gmv_loja = (
            df.groupby("loja")["valor_liquido"]
            .sum()
            .sort_values(ascending=True)
            .reset_index()
        )
        gmv_loja.columns = ["Loja", "GMV"]
        fig3 = px.bar(gmv_loja, x="GMV", y="Loja", orientation="h",
                      title="GMV por Loja", color="GMV",
                      color_continuous_scale="Reds", text_auto=".2s")
        fig3.update_layout(template="plotly_dark", showlegend=False, height=380)
        st.plotly_chart(fig3, use_container_width=True)

    with col_l2:
        # Meta vs Realizado — top 15 vendedores por meta
        meta_df = gmv_vend.sort_values("Meta", ascending=False).head(15)
        fig4 = go.Figure()
        fig4.add_bar(
            y=meta_df["vendedor"], x=meta_df["Meta"],
            name="Meta", orientation="h", marker_color="#555",
            opacity=0.6,
        )
        fig4.add_bar(
            y=meta_df["vendedor"], x=meta_df["GMV"],
            name="Realizado", orientation="h",
            marker_color=meta_df["Cor"].tolist(),
        )
        fig4.update_layout(
            title="Meta vs Realizado (Top 15)",
            barmode="overlay",
            template="plotly_dark",
            yaxis=dict(autorange="reversed"),
            height=420,
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    # ── Ticket médio e desconto por hora ─────────────────────────────────────
    col_t1, col_t2 = st.columns(2)

    with col_t1:
        ticket_h = (
            df.groupby("hora")
            .apply(lambda g: g["valor_liquido"].sum() / g["pedido_id"].nunique())
            .reindex(range(24), fill_value=0)
            .reset_index()
        )
        ticket_h.columns = ["Hora", "Ticket"]
        fig5 = px.line(ticket_h, x="Hora", y="Ticket",
                       title="Ticket Médio por Hora",
                       markers=True, color_discrete_sequence=["#FF4B4B"])
        fig5.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig5, use_container_width=True)

    with col_t2:
        desc_h = (
            df.groupby("hora")["desconto_pct"]
            .mean()
            .reindex(range(24), fill_value=0)
            .reset_index()
        )
        desc_h.columns = ["Hora", "Desconto (%)"]
        fig6 = px.area(desc_h, x="Hora", y="Desconto (%)",
                       title="Desconto Médio por Hora (%)",
                       color_discrete_sequence=["#FFA500"])
        fig6.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig6, use_container_width=True)

    # ── Tabela resumo vendedores ──────────────────────────────────────────────
    st.subheader("Ranking de Vendedores")
    tabela = gmv_vend[["vendedor", "GMV", "Meta", "Ating_%"]].copy()
    tabela.columns = ["Vendedor", "GMV (R$)", "Meta (R$)", "% Atingido"]
    tabela["GMV (R$)"]  = tabela["GMV (R$)"].map(lambda x: fmt_brl(x))
    tabela["Meta (R$)"] = tabela["Meta (R$)"].map(lambda x: fmt_brl(x))
    tabela["% Atingido"]= tabela["% Atingido"].map(lambda x: f"{x:.1f}%")
    tabela = tabela.reset_index(drop=True)
    tabela.index += 1
    st.dataframe(tabela, use_container_width=True, height=300)

# ╔══════════════════════════════════════════════════════════════════════════════
# ABA 3 — CONTROLADORIA
# ╚══════════════════════════════════════════════════════════════════════════════
with aba3:
    st.subheader("Controladoria & Resultado")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📊 Margem Bruta",      fmt_pct(margem_bruta))
    c2.metric("💸 Custo Total",       fmt_brl(custo_total))
    c3.metric("🏷️ Impacto Descontos", fmt_brl(impacto_desc))
    c4.metric("🚫 Cancelamentos",     f"{total_cancel:,} ({total_cancel/max(total_todas,1)*100:.1f}%)")

    st.divider()

    col_c1, col_c2 = st.columns(2)

    with col_c1:
        # Lucro por categoria
        lucro_cat = (
            df.groupby("categoria")
            .apply(lambda g: (g["valor_liquido"].sum() - g["custo_total"].sum()) * (1 - TAX_RATE))
            .reset_index()
        )
        lucro_cat.columns = ["Categoria", "Lucro Líquido"]
        lucro_cat = lucro_cat.sort_values("Lucro Líquido", ascending=True)
        lucro_cat["Cor"] = lucro_cat["Lucro Líquido"].apply(
            lambda x: "#2ecc71" if x >= 0 else "#e74c3c"
        )
        fig = px.bar(lucro_cat, x="Lucro Líquido", y="Categoria", orientation="h",
                     title="Lucro Líquido por Categoria", color="Cor",
                     color_discrete_map="identity", text_auto=".2s")
        fig.update_layout(template="plotly_dark", showlegend=False, height=360)
        st.plotly_chart(fig, use_container_width=True)

    with col_c2:
        # Lucro por canal
        lucro_canal = (
            df.groupby("canal")
            .apply(lambda g: (g["valor_liquido"].sum() - g["custo_total"].sum()) * (1 - TAX_RATE))
            .reset_index()
        )
        lucro_canal.columns = ["Canal", "Lucro Líquido"]
        fig2 = px.bar(lucro_canal, x="Canal", y="Lucro Líquido",
                      title="Lucro Líquido por Canal",
                      color="Canal", color_discrete_sequence=CORES,
                      text_auto=".2s")
        fig2.update_layout(template="plotly_dark", showlegend=False, height=360)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    col_c3, col_c4 = st.columns(2)

    with col_c3:
        # Cancelamentos e devoluções por categoria
        cancel_cat = (
            dft[dft["status"].isin(["Cancelado", "Devolvido"])]
            .groupby(["categoria", "status"])
            .size()
            .reset_index(name="Qtd")
        )
        if not cancel_cat.empty:
            fig3 = px.bar(cancel_cat, x="categoria", y="Qtd", color="status",
                          title="Cancelamentos e Devoluções por Categoria",
                          barmode="stack",
                          color_discrete_map={"Cancelado": "#e74c3c", "Devolvido": "#f39c12"})
            fig3.update_layout(template="plotly_dark", height=340)
            st.plotly_chart(fig3, use_container_width=True)

    with col_c4:
        # DRE simplificado
        receita_bruta = df["valor_liquido"].sum() / (1 - df["desconto_pct"].mean() / 100)
        receita_liq   = df["valor_liquido"].sum()
        lucro_bruto_v = receita_liq - custo_total
        impostos_est  = receita_liq * TAX_RATE
        lucro_op      = lucro_bruto_v - impostos_est

        dre = pd.DataFrame({
            "Item": [
                "Receita Bruta (estimada)",
                "(-) Descontos",
                "Receita Líquida (GMV)",
                "(-) Custo dos Produtos (CPV)",
                "Lucro Bruto",
                "(-) Impostos (~27%)",
                "Lucro Operacional",
            ],
            "Valor (R$)": [
                receita_bruta,
                -impacto_desc,
                receita_liq,
                -custo_total,
                lucro_bruto_v,
                -impostos_est,
                lucro_op,
            ],
        })
        dre["Valor (R$)"] = dre["Valor (R$)"].map(fmt_brl)

        st.subheader("DRE Simplificado")
        st.dataframe(dre, use_container_width=True, hide_index=True, height=290)

    # ── Evolução de margem por hora ───────────────────────────────────────────
    st.divider()
    margem_h = (
        df.groupby("hora")
        .apply(lambda g: (g["valor_liquido"].sum() - g["custo_total"].sum()) / g["valor_liquido"].sum() * 100
               if g["valor_liquido"].sum() > 0 else 0)
        .reindex(range(24), fill_value=0)
        .reset_index()
    )
    margem_h.columns = ["Hora", "Margem (%)"]
    fig_m = px.line(margem_h, x="Hora", y="Margem (%)",
                    title="Evolução da Margem Bruta por Hora",
                    markers=True, color_discrete_sequence=["#2ecc71"])
    fig_m.add_hline(y=margem_bruta, line_dash="dash", line_color="orange",
                    annotation_text=f"Média: {margem_bruta:.1f}%")
    fig_m.update_layout(template="plotly_dark", height=300)
    st.plotly_chart(fig_m, use_container_width=True)

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.divider()
total_db = query("SELECT COUNT(*) as n FROM vendas").iloc[0, 0]
st.caption(
    f"Total de transações no banco: **{total_db:,}** | "
    f"Concluídas: **{len(df):,}** | "
    f"Canceladas: **{total_cancel:,}** | "
    f"Devolvidas: **{total_dev:,}**"
)
