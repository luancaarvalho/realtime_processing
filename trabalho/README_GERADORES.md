# Geradores de Dados (Uso Rápido)

## O que existe
- `generate_seed_dataset.py`: cria dataset inicial (`orders_seed.csv` e `seller_targets.csv`).
- `bootstrap_seed.py`: publica o dataset inicial no Kafka e envia CSVs para MinIO.
- `stream_generator.py`: gera eventos contínuos em tempo real para o tópico Kafka.

## Pré-requisitos
- Kafka disponível.
- MinIO disponível (para o bootstrap enviar os CSVs de seed).
- Dependências Python instaladas (`trabalho/app/requirements.txt`).

## Ordem recomendada
1. Gerar seed:
```bash
python src/generate_seed_dataset.py
```
2. Publicar seed no Kafka:
```bash
python src/bootstrap_seed.py
```
3. Iniciar geração contínua:
```bash
python src/stream_generator.py
```

## Configuração por ambiente
Os scripts usam variáveis de ambiente definidas em `src/settings.py` (topic Kafka, endpoint/bucket MinIO, caminhos dos arquivos seed e taxa de geração).

## Resultado esperado
- Tópico Kafka recebe eventos de vendas de Black Friday.
- CSVs de seed ficam no MinIO em `seed/`.
- Fluxo contínuo mantém novos eventos sendo produzidos em tempo real.
