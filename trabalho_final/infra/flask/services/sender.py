from __future__ import annotations

import requests


class DjangoIngestionClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def send_order(self, payload: dict) -> dict:
        response = requests.post(
            f"{self.base_url}/sales/ingestion/",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
