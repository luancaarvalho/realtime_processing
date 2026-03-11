from __future__ import annotations

import random
import threading
import time

from .generator import RandomOrderGenerator
from .sender import DjangoIngestionClient


class TrafficSimulator:
    def __init__(self, base_url: str):
        self.client = DjangoIngestionClient(base_url=base_url)
        self.running = False
        self.thread = None
        self.stats = {
            "sent": 0,
            "created": 0,
            "duplicated": 0,
            "errors": 0,
            "last_error": None,
        }

    def _current_batch_size(self) -> int:
        # simula oscilação de tráfego
        return random.randint(1, 6)

    def _loop(self):
        while self.running:
            batch_size = self._current_batch_size()

            for _ in range(batch_size):
                payload = RandomOrderGenerator.generate_order()
                try:
                    result = self.client.send_order(payload)
                    self.stats["sent"] += 1
                    if result.get("created"):
                        self.stats["created"] += 1
                    if result.get("duplicated"):
                        self.stats["duplicated"] += 1
                except Exception as exc:
                    self.stats["errors"] += 1
                    self.stats["last_error"] = str(exc)

            time.sleep(random.uniform(0.3, 1.2))

    def start(self):
        if self.running:
            return False

        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.running = False
        return True

    def get_status(self) -> dict:
        return {
            "running": self.running,
            **self.stats,
        }
