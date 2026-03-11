import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_ORDER_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders.v1")
    DJANGO_INGEST_URL = os.getenv(
        "DJANGO_INGEST_URL",
        "http://localhost:8000/sales/ingest/orders/",
    )
    DJANGO_API_KEY = os.getenv("DJANGO_API_KEY", "")
