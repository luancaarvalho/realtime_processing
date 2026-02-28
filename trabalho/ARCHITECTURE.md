# Arquitetura — Simulação Black Friday (Monitor de Vendas)

## Visão Geral

Este repositório implementa um pipeline de dados simplificado de ponta a ponta para simular um cenário de Black Friday: geração de pedidos (seed e em tempo real), publicação em Kafka, processamento/ingestão para armazenamento analítico (DuckDB) e visualização por um dashboard (Streamlit).

Objetivo: demonstrar fluxo fim-a-fim com métricas de vendas, controle e KPIs executivos.

## Componentes

- Geradores
  - `generate_seed_dataset.py` — cria arquivos CSV de seed (`orders_seed.csv`, `seller_targets.csv`).
  - `bootstrap_seed.py` — publica a seed no tópico Kafka e envia os CSVs para MinIO (bucket `seed/`).
  - `stream_generator.py` — gerador contínuo que produz eventos no tópico Kafka em tempo real.

- Mensageria
  - Kafka: tópico default `blackfriday_orders` (configurado em `src/settings.py`).

- Processamento
  - `stream_processor.py` — consumidor Kafka que escreve/append dos eventos em uma tabela `sales` em DuckDB (`/app/data/blackfriday.db`).

- Armazenamento
  - DuckDB: armazenamento analítico local em arquivo para leitura pelo dashboard.
  - MinIO (S3 compatível): armazena os CSVs seed e artefatos (opcional). Configurado via `minio_utils.py`.

- Visualização
  - `dashboard.py` (Streamlit) — lê diretamente a tabela `sales` em DuckDB e apresenta KPIs, gráficos de vendas e controladoria.

- Utilitários
  - `kafka_utils.py` — ajuda para criar/verificar tópicos e disponibilidade de brokers.
  - `minio_utils.py` — funções para upload/download em MinIO.
  - `event_factory.py` / `catalog_data.py` — lógica de geração de eventos e catálogos (sellers, produtos, targets).

## Fluxo de Dados (sequência)

1. Seed: `generate_seed_dataset.py` → gera `orders_seed.csv` e `seller_targets.csv` em `/app/data/seed/`.
2. Bootstrap: `bootstrap_seed.py` publica cada linha de `orders_seed.csv` no tópico Kafka e envia os CSVs para MinIO.
3. Produção contínua: `stream_generator.py` produz eventos ao tópico Kafka em tempo real (configurável via `GENERATOR_EVENTS_PER_SECOND`).
4. Processamento: `stream_processor.py` consome o tópico e faz INSERTs na tabela `sales` em DuckDB.
5. Dashboard: `dashboard.py` lê DuckDB e apresenta métricas; o usuário pode atualizar via botão ou configurar autorefresh.

## Diagrama (Mermaid)

```mermaid
flowchart LR
  subgraph Producers
    A[generate_seed_dataset / bootstrap_seed]
    B[stream_generator]
  end
  A -->|publish seed| K[Kafka: blackfriday_orders]
  B -->|produce events| K
  K --> C[stream_processor]
  C --> D[DuckDB: /app/data/blackfriday.db]
  D --> E[Streamlit Dashboard]
  A --> F[MinIO (seed/)]
```

## Principais arquivos e onde olhar

- Dashboard: `app/src/dashboard.py` — leitura DuckDB e visualização.  
- Processador: `app/src/stream_processor.py` — consumo Kafka → DuckDB.  
- Geradores: `app/src/generate_seed_dataset.py`, `app/src/bootstrap_seed.py`, `app/src/stream_generator.py`.  
- Config: `app/src/settings.py` — variáveis e caminhos (KAFKA, MINIO, paths locais).  

## Como executar (local / Docker Compose)

Requisitos: Docker Compose com serviços Kafka, Zookeeper e MinIO disponíveis (provavelmente já definidos no repo principal). As variáveis padrão em `settings.py` assumem nomes de host usados em compose (`kafka:29092`, `minio:9000`).

Exemplo (modo rápido usando o compose do repositório):

```bash
# no diretório raiz do workspace
docker compose up --build

# gerar seed local (opcional, se quiser gerar novos arquivos):
python app/src/generate_seed_dataset.py

# preparar tópico (cria topic e arquivos de seed se necessário):
python app/src/topic_setup.py

# publicar seed no Kafka e enviar CSVs ao MinIO:
python app/src/bootstrap_seed.py

# iniciar processor (consome e popula DuckDB):
python app/src/stream_processor.py

# iniciar gerador contínuo (opcional):
python app/src/stream_generator.py

# iniciar dashboard (Streamlit):
streamlit run app/src/dashboard.py --server.port 8505
```

Observação: se estiver rodando em containers, adapte caminhos e variáveis de ambiente conforme o `Dockerfile` e compose.
