# Demo 2 - DuckDB (Batch) via Python e CLI SQL

Demo de apresentacao (sem streaming): cria tabelas `bronze_sales` e `gold_kpis` em um banco DuckDB local, consulta via Python e via CLI.

## Como usar

1. Criar ambiente (uma vez, na pasta `aula06`):

```bash
conda env create -f ../environment.yml
conda activate aula06-small-data
```

Diretorio de execucao dos comandos abaixo:

```bash
cd /Users/luancarvalho/Documents/Unifor/realtime_processing/aula06/demo_duckdb_batch
```

2. Criar base e tabelas:

```bash
python setup_demo.py
```

3. Rodar consultas via Python:

```bash
python demo_python.py
```

4. Rodar consultas via DuckDB CLI + SQL:

```bash
duckdb data/small_data.duckdb -f sql/demo_cli.sql
```

Se o binario `duckdb` nao estiver no PATH, use:

```bash
python run_cli_sql.py
```

Ou execute sem ativar ambiente:

```bash
conda run -n aula06-small-data python setup_demo.py
conda run -n aula06-small-data duckdb data/small_data.duckdb -f sql/demo_cli.sql
```

## Principais comandos para ver as tabelas (CLI interativo)

```sql
SHOW TABLES;
SELECT * FROM bronze_sales LIMIT 10;
SELECT * FROM gold_kpis ORDER BY gmv_total DESC;
DESCRIBE bronze_sales;
```
