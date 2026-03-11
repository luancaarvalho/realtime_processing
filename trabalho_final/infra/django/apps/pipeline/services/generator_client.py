import requests
from django.conf import settings


class GeneratorClient:
    def __init__(self) -> None:
        self.base_url = settings.GENERATOR_SERVICE_URL.rstrip("/")

    def start(self) -> dict:
        response = requests.post(
            f"{self.base_url}/producer/start",
            timeout=300,
        )
        response.raise_for_status()
        return response.json()

    def stop(self) -> dict:
        response = requests.post(
            f"{self.base_url}/producer/stop",
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def status(self) -> dict:
        response = requests.get(
            f"{self.base_url}/producer/status",
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
