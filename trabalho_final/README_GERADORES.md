# Geradores de Dados - Trabalho Final

## Scripts
- `src/generate_seed_dataset.py`: gera `orders_seed.csv` e `seller_targets.csv`.
- `src/bootstrap_seed.py`: publica seed no Kafka e sobe CSVs no MinIO.
- `src/stream_generator.py`: gera eventos continuos no Kafka.
- `src/stream_processor.py`: consome Kafka, calcula KPIs e salva parquet + JSON no MinIO.
- `src/dashboard.py`: dashboard Streamlit com refresh automatico.

## Ordem
A ordem ja esta automatizada no `docker-compose.yml` do `trabalho_final`.

## Chaves MinIO
- parquet processado: `processed/orders/...`
- metricas dashboard: `metrics/dashboard_latest.json`
- arquivos seed: `seed/orders_seed.csv` e `seed/seller_targets.csv`
