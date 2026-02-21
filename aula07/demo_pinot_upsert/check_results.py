from __future__ import annotations

import argparse
import time
from typing import Any

import requests

BROKER_SQL = "http://localhost:18099/query/sql"
TABLE = "pinot_upsert_orders"


def run_sql(sql: str) -> list[list[Any]]:
    resp = requests.post(BROKER_SQL, json={"sql": sql}, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("resultTable", {}).get("rows", [])


def wait_for_count(expected: int, timeout_s: int = 90) -> int:
    start = time.time()
    while True:
        rows = run_sql(f"SELECT COUNT(*) FROM {TABLE}")
        count = int(rows[0][0]) if rows else 0
        if count >= expected:
            return count

        if time.time() - start > timeout_s:
            return count
        time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida resultado da demo Pinot Upsert")
    parser.add_argument("--expected", type=int, default=12)
    args = parser.parse_args()

    total = wait_for_count(args.expected)
    print(f"Total de linhas no Pinot: {total}")

    order3 = run_sql(
        f"SELECT order_id, customer_id, status, amount FROM {TABLE} WHERE order_id = 3 LIMIT 1"
    )
    print(f"Linha do order_id=3 (deve estar atualizada): {order3}")

    all_ids = run_sql(f"SELECT order_id FROM {TABLE} ORDER BY order_id LIMIT 100")
    print(f"IDs finais: {[row[0] for row in all_ids]}")


if __name__ == "__main__":
    main()
