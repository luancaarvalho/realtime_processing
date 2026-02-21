# Demo 1 - Polars + Delta (Batch)

Demo de apresentacao (sem streaming): cria um batch, escreve `bronze` em Delta, agrega e escreve `gold` em Delta.

## Como usar

1. Criar ambiente (uma vez, na pasta `aula06`):

```bash
conda env create -f ../environment.yml
conda activate aula06-small-data
```

2. Rodar a demo:

```bash
python run_demo.py
```

## Principais comandos para ver as tabelas

```python
import polars as pl

pl.read_delta("data/bronze_sales_delta").head(10)
pl.read_delta("data/gold_kpis_delta").sort("gmv_total", descending=True)
```
