# Demo Pinot Upsert (10 + 3)

Demo simples para mostrar upsert no Pinot:
- carga inicial: 10 linhas fixas
- upsert: 3 linhas fixas (1 update + 2 inserts)
- resultado final esperado: 12 linhas

## 1) Subir infraestrutura (na raiz do projeto)

Opção A (compose central):

```bash
cd /Users/luancarvalho/Documents/Unifor/realtime_processing
docker network create kafka_net || true
docker compose up -d zookeeper kafka kafka-2 kafka-3 ksqldb-server ksqldb-cli pinot-controller pinot-broker pinot-server
```

Opção B (compose mínimo só para Pinot):

```bash
cd /Users/luancarvalho/Documents/Unifor/realtime_processing
docker compose -f docker-compose.pinot.yml up -d
```

## 2) Instalar dependências da demo

```bash
cd /Users/luancarvalho/Documents/Unifor/realtime_processing/aula07/demo_pinot_upsert
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Configurar topic + schema/table no Pinot

```bash
python setup_pinot.py
```

No compose mínimo (`docker-compose.pinot.yml`), use:

```bash
python setup_pinot.py --bootstrap localhost:9092
```

## 4) Enviar os 10 registros iniciais

```bash
python send_orders.py
```

No compose mínimo, use:

```bash
python send_orders.py --bootstrap localhost:9092
```

## 5) Enviar os 3 de upsert

```bash
python send_orders.py --upsert
```

No compose mínimo, use:

```bash
python send_orders.py --upsert --bootstrap localhost:9092
```

## 6) Validar resultado

```bash
python check_results.py --expected 12
```

Esperado:
- `Total de linhas no Pinot: 12`
- `order_id=3` com dados atualizados (status/amount)
- IDs finais contendo `1..12`

## Dados explícitos usados na aula
- `send_orders.py` contém a lista fixa `INITIAL_10`.
- `send_orders.py --upsert` usa a lista fixa `UPSERT_3`.
