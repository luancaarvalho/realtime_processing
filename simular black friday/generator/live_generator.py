# bibliotecas
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from event_factory import build_order_event

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1),
    request_timeout_ms=30000
)

TOPIC = "black_friday"

def start_stream():

    rng = random.Random()

    while True:

        order_id = str(uuid.uuid4())
        event_time = datetime.now()

        event = build_order_event(
            rng=rng,
            order_id=order_id,
            event_time=event_time
        )

        future = producer.send(TOPIC, event)

        producer.flush()

        #para verificar o envio
        print(f"Evento processado: {event['order_id']}")

        time.sleep(0.5)

if __name__ == "__main__":
    start_stream()