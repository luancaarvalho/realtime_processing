import orjson
import duckdb
from confluent_kafka import Consumer
from settings import SETTINGS


def main():
    db_path = "/app/data/blackfriday.db"

    db = duckdb.connect(db_path)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            event_id VARCHAR, event_time TIMESTAMP, order_id VARCHAR,
            seller_name VARCHAR, store_name VARCHAR, category VARCHAR,
            channel VARCHAR, gross_amount DOUBLE, net_revenue DOUBLE,
            cogs DOUBLE, discount_amount DOUBLE, status VARCHAR,
            target_gmv DOUBLE, target_orders INTEGER
        )
    """
    )
    db.close()

    c = Consumer(
        {
            "bootstrap.servers": SETTINGS.kafka_bootstrap_servers,
            "group.id": "processor_main_group",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([SETTINGS.kafka_topic_orders])

    print("--- Processador Iniciado: Lendo do Kafka... ---")

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro Kafka: {msg.error()}")
            continue

        data = orjson.loads(msg.value())

        db = duckdb.connect(db_path)
        db.execute(
            "INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                data["event_id"],
                data["event_time"],
                data["order_id"],
                data["seller_name"],
                data["store_name"],
                data["category"],
                data["channel"],
                data["gross_amount"],
                data["net_revenue"],
                data["cogs"],
                data["discount_amount"],
                data["status"],
                data.get("target_gmv", 0),
                data.get("target_orders", 0),
            ],
        )
        db.close()


if __name__ == "__main__":
    main()
