import json
import duckdb
import polars as pl
import os
from kafka import KafkaConsumer

os.makedirs("database", exist_ok=True)

TOPIC = "black_friday"
BOOTSTRAP_SERVER = "127.0.0.1:9092"
DB_PATH = "database/black_friday.duckdb"

# cria a tabela e fecha para não ter conflito
with duckdb.connect(DB_PATH) as con:
    con.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        order_id VARCHAR,
        event_time TIMESTAMP,
        seller_id VARCHAR,
        store_id VARCHAR,
        category VARCHAR,
        channel VARCHAR,
        quantity INTEGER,
        unit_price DOUBLE,
        gross_value DOUBLE,
        discount_value DOUBLE,
        net_revenue DOUBLE,
        cost DOUBLE,
        profit DOUBLE,  -- Ajustado para bater com seu script do Streamlit
        status VARCHAR  -- Adicionado para os filtros de cancelado/devolvido que você usou
    )
    """)

# consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="black_friday_duckdb_consumer"
)

print(f"Consumer iniciado em {BOOTSTRAP_SERVER}...")

try:
    con = duckdb.connect(DB_PATH)
    
    for message in consumer:
        event = message.value
        
        df_event = pl.DataFrame([event])
        
        con.append("sales", df_event.to_pandas())
        
        con.execute("CHECKPOINT") 
        
        print(f"Pedido {event.get('order_id')} processado.")

except Exception as e:
    print(f"Erro: {e}")
finally:
    if 'con' in locals():
        con.close()