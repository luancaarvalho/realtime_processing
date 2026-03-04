# Trabalho Final - Black Friday Streaming

Projeto de processamento em tempo real com Kafka, MinIO e dashboard em Streamlit.

## Tecnologias
- Kafka (streaming de eventos)
- Python (gerador, bootstrap e processamento)
- MinIO (armazenamento de seed, parquet e metricas)
- Streamlit + Plotly (dashboard em tempo real)

## Arquitetura
1. `bf-topic-init`
   - Garante disponibilidade do Kafka.
   - Cria topico `blackfriday_orders`.
   - Gera seed caso necessario.
2. `bf-seed-bootstrap`
   - Publica seed no Kafka.
   - Sobe `orders_seed.csv` e `seller_targets.csv` para MinIO.
3. `bf-generator`
   - Continua gerando eventos em tempo real no Kafka.
4. `bf-processor`
   - Consome eventos do Kafka.
   - Calcula KPIs (C-Level, Vendas, Controladoria).
   - Salva lotes parquet em MinIO (`processed/orders/...`).
   - Publica metricas para dashboard (`metrics/dashboard_latest.json`).
5. `bf-dashboard`
   - Le metricas no MinIO.
   - Renderiza painel unico com atualizacao automatica.

## Estrutura
- `trabalho_final/docker-compose.yml`
- `trabalho_final/app/Dockerfile`
- `trabalho_final/app/src/*.py`
- `trabalho_final/data/seed/*.csv`

## Como executar
No terminal:

```bash
cd trabalho_final
docker compose up -d --build
```

## Acessos
- Dashboard: `http://localhost:18501`
- MinIO Console: `http://localhost:19001`
  - user: `minioadmin`
  - pass: `minioadmin`
- Kafka (host): `localhost:19092`

## Comandos uteis
Status dos servicos:

```bash
docker compose ps
```

Logs:

```bash
docker compose logs -f kafka
docker compose logs -f bf-topic-init
docker compose logs -f bf-seed-bootstrap
docker compose logs -f bf-generator
docker compose logs -f bf-processor
docker compose logs -f bf-dashboard
```

Parar tudo:

```bash
docker compose down
```

## Dashboard sem F5
Sim. O dashboard atualiza automaticamente a cada 5 segundos (auto refresh), sem precisar recarregar manualmente.

## Troubleshooting rapido
Se o dashboard nao abrir:
1. Verifique se `kafka` esta `healthy` no `docker compose ps`.
2. Verifique se `bf-topic-init` finalizou com sucesso.
3. Verifique logs do processor (`bf-processor`) para confirmar escrita de metricas no MinIO.

Se quiser resetar ambiente:

```bash
docker compose down -v
docker compose up -d --build
```
