import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import os
import time

st.set_page_config(page_title="Black Friday Real-Time Dashboard", layout="wide")

def get_db_connection():
    return mysql.connector.connect(
        host="mysql",
        user="demo_user",
        password="demo_password",
        database="demo_db"
    )

def load_data(table_name):
    try:
        conn = get_db_connection()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

st.title("🚀 Black Friday Real-Time Sales Dashboard")

# Sidebar for refresh interval
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 5, 60, 10)

# Layout
col1, col2, col3 = st.columns(3)

# 1. C-Level Metrics
df_c_level = load_data("kpi_c_level")
if not df_c_level.empty:
    # Get latest metrics
    latest = df_c_level.sort_values("calculation_time", ascending=False).iloc[0]
    col1.metric("Total GMV", f"R$ {latest['gmv']:,.2f}")
    col2.metric("Net Profit", f"R$ {latest['net_profit']:,.2f}")
    col3.metric("Avg Ticket", f"R$ {latest['avg_ticket']:,.2f}")

st.divider()

# 2. Sales Metrics
st.header("📊 Sales Performance")
df_sales = load_data("kpi_sales_by_seller")
if not df_sales.empty:
    # Aggregate by seller (since we append batches)
    seller_agg = df_sales.groupby("seller_name").agg({
        "gmv": "sum",
        "num_orders": "sum"
    }).reset_index().sort_values("gmv", ascending=False)
    
    fig_seller = px.bar(seller_agg, x="seller_name", y="gmv", title="GMV by Seller")
    st.plotly_chart(fig_seller, use_container_width=True)

st.divider()

# 3. Controladoria
st.header("🛡️ Controladoria & Margens")
df_control = load_data("kpi_controladoria")
if not df_control.empty:
    cat_agg = df_control.groupby("category").agg({
        "net_revenue": "sum",
        "total_cost": "sum",
        "cancelled_count": "sum"
    }).reset_index()
    cat_agg["profit"] = cat_agg["net_revenue"] - cat_agg["total_cost"]
    
    fig_profit = px.pie(cat_agg, values="profit", names="category", title="Profit by Category")
    st.plotly_chart(fig_profit, use_container_width=True)

# Auto-refresh
time.sleep(refresh_rate)
st.rerun()
