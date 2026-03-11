"""
generator.py — Gerador contínuo de transações de Black Friday.
Simula picos de demanda ao longo do dia e grava no SQLite em tempo real.
"""
import sqlite3
import random
import time
import uuid
from datetime import datetime, timedelta

import os
DB_PATH      = os.environ.get("DB_PATH", "blackfriday.db")
BATCH_SIZE   = 5          # transações por ciclo
INTERVAL_SEC = 1.5        # segundos entre batches
TAX_RATE     = 0.27       # alíquota de impostos simulada (ICMS + PIS/COFINS)

CANAIS = ["E-commerce", "App Mobile", "Loja Física", "Televendas"]

PESO_STATUS = [
    ("Concluído",  0.88),
    ("Cancelado",  0.07),
    ("Devolvido",  0.05),
]
STATUSES = [s for s, _ in PESO_STATUS]
WEIGHTS  = [w for _, w in PESO_STATUS]

# Descontos típicos de Black Friday por categoria
DESC_RANGE = {
    "Eletrônicos":      (10, 40),
    "Eletrodomésticos": (10, 35),
    "Moda":             (20, 60),
    "Beleza":           (15, 50),
    "Esportes":         (10, 45),
    "Casa & Decoração": (10, 40),
    "Livros":           (10, 30),
    "Brinquedos":       (15, 50),
}


def load_master_data(conn):
    vendedores = conn.execute("SELECT id, loja_id FROM vendedores").fetchall()
    produtos   = conn.execute(
        "SELECT id, categoria, custo_unitario, preco_base FROM produtos"
    ).fetchall()
    return vendedores, produtos


def hora_peso(hora: int) -> float:
    """Retorna peso relativo de volume para cada hora do dia (pico Black Friday)."""
    picos = {
        0: 0.3, 1: 0.1, 2: 0.05, 3: 0.05, 4: 0.05, 5: 0.1,
        6: 0.2, 7: 0.4, 8: 0.7,  9: 1.2, 10: 1.5, 11: 1.8,
        12: 2.0, 13: 1.7, 14: 1.5, 15: 1.6, 16: 1.8, 17: 2.0,
        18: 2.5, 19: 2.8, 20: 3.0, 21: 2.5, 22: 2.0, 23: 1.5,
    }
    return picos.get(hora, 1.0)


def gerar_venda(vendedores, produtos, ts: datetime) -> dict:
    vendedor_id, loja_id = random.choice(vendedores)
    prod_id, categoria, custo_unit, preco_base = random.choice(produtos)

    desc_min, desc_max = DESC_RANGE.get(categoria, (10, 40))
    desconto_pct  = round(random.uniform(desc_min, desc_max), 2)
    quantidade    = random.choices([1, 2, 3, 4, 5], weights=[55, 25, 10, 6, 4])[0]
    preco_venda   = round(preco_base * (1 - desconto_pct / 100), 2)
    valor_liquido = round(preco_venda * quantidade, 2)
    custo_total   = round(custo_unit * quantidade, 2)

    # Canal: lojas físicas vendem mais em loja; online tem canais digitais
    if loja_id in (9, 10):
        canal = random.choice(["E-commerce", "App Mobile"])
    else:
        canal = random.choices(
            ["Loja Física", "E-commerce", "App Mobile", "Televendas"],
            weights=[60, 20, 15, 5]
        )[0]

    status = random.choices(STATUSES, weights=WEIGHTS)[0]

    return {
        "pedido_id":     str(uuid.uuid4())[:8].upper(),
        "timestamp":     ts.strftime("%Y-%m-%d %H:%M:%S"),
        "vendedor_id":   vendedor_id,
        "loja_id":       loja_id,
        "produto_id":    prod_id,
        "canal":         canal,
        "quantidade":    quantidade,
        "preco_unitario": preco_venda,
        "desconto_pct":  desconto_pct,
        "valor_liquido": valor_liquido,
        "custo_total":   custo_total,
        "status":        status,
    }


def inserir_vendas(conn, vendas: list):
    conn.executemany(
        """INSERT INTO vendas
           (pedido_id, timestamp, vendedor_id, loja_id, produto_id,
            canal, quantidade, preco_unitario, desconto_pct,
            valor_liquido, custo_total, status)
           VALUES
           (:pedido_id,:timestamp,:vendedor_id,:loja_id,:produto_id,
            :canal,:quantidade,:preco_unitario,:desconto_pct,
            :valor_liquido,:custo_total,:status)""",
        vendas,
    )
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    vendedores, produtos = load_master_data(conn)

    # Simula que a Black Friday começou às 00h; avança o relógio virtual
    sim_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    sim_time  = sim_start
    total     = 0

    print("=== Gerador Black Friday iniciado ===", flush=True)
    print(f"Intervalo: {INTERVAL_SEC}s | Batch: {BATCH_SIZE} transações", flush=True)
    print("Pressione Ctrl+C para encerrar.\n", flush=True)

    try:
        while True:
            hora   = sim_time.hour
            peso   = hora_peso(hora)
            n      = max(1, int(BATCH_SIZE * peso * random.uniform(0.7, 1.3)))
            vendas = [gerar_venda(vendedores, produtos, sim_time) for _ in range(n)]

            inserir_vendas(conn, vendas)
            total += n

            agora = datetime.now().strftime("%H:%M:%S")
            print(f"[{agora}] sim={sim_time.strftime('%H:%M')} | +{n} vendas | total={total:,}", flush=True)

            # Avança o relógio simulado (~4 minutos por segundo real)
            sim_time += timedelta(minutes=4)
            if sim_time.date() > sim_start.date():
                sim_time = sim_start  # reinicia o ciclo diário

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nGerador encerrado. Total inserido: {total:,} transações.", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
