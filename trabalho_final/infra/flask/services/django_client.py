from __future__ import annotations

import requests

from config.settings import Config


class DjangoIngestClient:
    def __init__(self) -> None:
        self.url = Config.DJANGO_INGEST_URL
        self.api_key = Config.DJANGO_API_KEY

    def send_order(self, payload: dict) -> dict:
        headers = {
            "Content-Type": "application/json",
        }

        if self.api_key:
            headers["X-API-Key"] = self.api_key

        response = requests.post(
            self.url,
            json=payload,
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()
