from __future__ import annotations

import csv
import random
from datetime import UTC, datetime
from pathlib import Path

from catalog_data import default_targets
from event_factory import build_order_event, random_black_friday_timestamp
from settings import SETTINGS


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError("No rows to write")

    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(seed_size: int = 18000, seed: int = 42) -> None:
    rng = random.Random(seed)

    targets = default_targets()
    target_map = {str(item["seller_id"]): item for item in targets}

    reference_date = datetime(2025, 11, 28, tzinfo=UTC)
    rows: list[dict[str, object]] = []

    for idx in range(seed_size):
        event_ts = random_black_friday_timestamp(rng, reference_date)
        order_id = f"SEED-{idx + 1:07d}"
        rows.append(build_order_event(rng, order_id, event_ts, target_map))

    rows.sort(key=lambda x: str(x["event_time"]))

    orders_path = Path(SETTINGS.seed_orders_path)
    targets_path = Path(SETTINGS.seed_targets_path)

    _write_csv(orders_path, rows)
    _write_csv(targets_path, targets)

    print(
        f"Seed dataset generated: orders={len(rows)} at {orders_path} | targets={len(targets)} at {targets_path}"
    )


if __name__ == "__main__":
    main()
