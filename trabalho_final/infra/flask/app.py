from __future__ import annotations

import random
import threading
import time
from typing import Any

from flask import Flask, jsonify, request

from kafka.consumer import OrderConsumer
from kafka.producer import OrderProducer
from payloads.sample_order import build_sample_order_payload

app = Flask(__name__)

producer = OrderProducer()

SIMULATOR_RUNNING = False
SIMULATOR_THREAD = None
SIMULATOR_LOCK = threading.Lock()

SIMULATOR_STATS: dict[str, Any] = {
    "running": False,
    "sent": 0,
    "errors": 0,
    "last_error": None,
    "last_external_id": None,
    "last_event_id": None,
}


def _build_random_payload() -> dict:
    """
    Gera um payload fictício usando o builder atual.
    O ideal é que o build_sample_order_payload() já gere external_id/event_id novos.
    """
    return build_sample_order_payload()


def _publish_payload(payload: dict) -> None:
    producer.send(payload)
    producer.flush()

    SIMULATOR_STATS["sent"] += 1
    SIMULATOR_STATS["last_external_id"] = payload.get("external_id")
    SIMULATOR_STATS["last_event_id"] = payload.get("event_id")


def _traffic_simulator_loop() -> None:
    global SIMULATOR_RUNNING

    while True:
        with SIMULATOR_LOCK:
            if not SIMULATOR_RUNNING:
                SIMULATOR_STATS["running"] = False
                break

        try:
            batch_size = random.randint(1, 5)

            for _ in range(batch_size):
                payload = _build_random_payload()
                _publish_payload(payload)

            time.sleep(random.uniform(0.3, 1.2))

        except Exception as exc:
            SIMULATOR_STATS["errors"] += 1
            SIMULATOR_STATS["last_error"] = str(exc)
            time.sleep(1)


@app.post("/publish-order")
def publish_order():
    """
    Publica um único pedido no Kafka.
    Se não vier payload no body, gera um fictício automaticamente.
    """
    try:
        payload = request.get_json(silent=True) or _build_random_payload()
        _publish_payload(payload)

        return jsonify(
            {
                "success": True,
                "message": "Pedido publicado no Kafka com sucesso.",
                "external_id": payload.get("external_id"),
                "event_id": payload.get("event_id"),
            }
        ), 202
    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "message": "Erro ao publicar pedido no Kafka.",
                "detail": str(exc),
            }
        ), 500


@app.post("/producer/start")
def start_producer():
    """
    Inicia a simulação contínua em background.
    """
    global SIMULATOR_RUNNING, SIMULATOR_THREAD

    with SIMULATOR_LOCK:
        if SIMULATOR_RUNNING:
            return jsonify(
                {
                    "success": True,
                    "message": "Producer já está em execução.",
                    "status": SIMULATOR_STATS,
                }
            ), 200

        SIMULATOR_RUNNING = True
        SIMULATOR_STATS["running"] = True
        SIMULATOR_STATS["last_error"] = None

        SIMULATOR_THREAD = threading.Thread(
            target=_traffic_simulator_loop,
            daemon=True,
            name="traffic-simulator-thread",
        )
        SIMULATOR_THREAD.start()

    return jsonify(
        {
            "success": True,
            "message": "Simulação iniciada com sucesso.",
            "status": SIMULATOR_STATS,
        }
    ), 200


@app.post("/producer/stop")
def stop_producer():
    """
    Para a simulação contínua.
    """
    global SIMULATOR_RUNNING

    with SIMULATOR_LOCK:
        SIMULATOR_RUNNING = False
        SIMULATOR_STATS["running"] = False

    return jsonify(
        {
            "success": True,
            "message": "Simulação parada com sucesso.",
            "status": SIMULATOR_STATS,
        }
    ), 200


@app.get("/producer/status")
def producer_status():
    """
    Retorna o status atual do simulador.
    """
    return jsonify(
        {
            "success": True,
            "status": SIMULATOR_STATS,
        }
    ), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
