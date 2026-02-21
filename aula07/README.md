# Aula 07 - Pinot Upsert (KSQLDB + Pinot no Compose)

Esta aula mostra um fluxo simples de **upsert no Apache Pinot**:

- primeira carga: 10 registros fixos
- segunda carga (`--upsert`): 3 registros (1 update + 2 inserts)
- resultado final esperado: **12 linhas**

Demo da aula:
- `demo_pinot_upsert`
