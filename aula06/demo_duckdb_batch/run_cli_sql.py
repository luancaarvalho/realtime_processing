from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "data" / "small_data.duckdb"
DEFAULT_SQL = ROOT / "sql" / "demo_cli.sql"


def split_sql(sql_text: str) -> list[str]:
    return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]


def main() -> None:
    db_path = DEFAULT_DB
    sql_path = DEFAULT_SQL

    if not db_path.exists():
        raise SystemExit(f"Banco nao encontrado: {db_path}. Rode primeiro: python setup_demo.py")
    if not sql_path.exists():
        raise SystemExit(f"Arquivo SQL nao encontrado: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql(sql_text)

    con = duckdb.connect(str(db_path))
    try:
        for idx, stmt in enumerate(statements, start=1):
            print("\n" + "=" * 80)
            print(f"[{idx}] SQL")
            print("=" * 80)
            print(stmt + ";")
            rel = con.sql(stmt)
            rows = rel.fetchall()
            if rows:
                for row in rows:
                    print(row)
            else:
                print("(ok)")
    finally:
        con.close()


if __name__ == "__main__":
    main()
