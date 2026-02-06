# TODO / Estado atual

## Contexto
- Trabalhando em `/Users/luancarvalho/Documents/Unifor/realtime_processing`.
- Usar **somente** o compose: `/Users/luancarvalho/Documents/Unifor/realtime_processing/docker-compose.yml`.

## O que foi feito
- Ajustado o `docker-compose.yml` para ksqlDB:
  - `ksqldb-server` e `ksqldb-cli` agora usam `confluentinc/cp-ksqldb-server:7.4.3` e `confluentinc/cp-ksqldb-cli:7.4.3`.
  - rede default apontando para `kafka_net` (externa).
  - setadas vars no `ksqldb-server`:
    - `KSQL_KSQL_INTERNAL_TOPIC_REPLICAS=3`
    - `KSQL_KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS=3`
    - `KSQL_KSQL_STREAMS_REPLICATION_FACTOR=3`
- Subi **somente** ksqlDB via compose (sem deps):
  - `docker compose up -d --no-deps ksqldb-server ksqldb-cli`
  - Depois de trocar as imagens para `cp-ksqldb-*`, o endpoint respondeu: `http://localhost:8088/info` retornou `RUNNING`.
- Aula02 demo01 validada parcialmente:
  - `aula02/demo01/demo01_producer.py` rodou com `--minutes 1 --files_per_min 20` e publicou em `rt.files.raw`.
  - `aula02/demo01/ksql_demo01.sql` foi corrigido (campo `size` virou `` `size` ``) e executado com sucesso no ksqlDB.
  - Stream `files_raw` e tabela `files_per_min` criados.
  - Query `SELECT * FROM files_per_min WHERE user_id='user_27';` retornou linhas.
- Foram abertas algumas queries PUSH e depois encerradas com `TERMINATE ...`.

## Estado atual (ponto de parada)
- ksqlDB respondeu antes como `RUNNING`, mas após terminar queries o `/info` está com `serverStatus: ERROR`.
- `docker-compose.yml` está modificado conforme acima.
- Demo02 (Aula02) **ainda não validada**.

## O que falta fazer
1. **Normalizar status do ksqlDB**
   - Checar `curl http://localhost:8088/info`.
   - Se continuar `ERROR`, reiniciar apenas ksqlDB:
     - `docker compose -f /Users/luancarvalho/Documents/Unifor/realtime_processing/docker-compose.yml rm -f ksqldb-server ksqldb-cli`
     - `docker compose -f /Users/luancarvalho/Documents/Unifor/realtime_processing/docker-compose.yml up -d --no-deps ksqldb-server ksqldb-cli`
   - Verificar novamente `RUNNING`.
2. **Validar Aula02 demo01 (end-to-end)**
   - Rodar producer (ex.: 1 min, 20 arquivos) e conferir resultado com SELECT/PRINT na tabela `files_per_min`.
3. **Validar Aula02 demo02**
   - Rodar `aula02/demo02/demo02_producer.py`.
   - Executar `ksql_stream.sql` (cria stream base) e depois **uma** das execuções de grace: `ksql_grace_0.sql`, `ksql_grace_5.sql`, `ksql_grace_20.sql`.
   - Observar mudanças no resultado (janela 10s) conforme grace.
4. **Atualizar guia.md**
   - Conferir se comandos da Aula02 batem com SQLs/caminhos atuais e com ksqlDB CLI 7.4.3.

## Evidências já obtidas
- `demo01_producer.py` enviou eventos (kcat confirmou mensagens em `rt.files.raw`).
- `ksql_demo01.sql` criou stream e tabela sem erro.
- Query por `user_id` retornou linhas em `files_per_min`.
