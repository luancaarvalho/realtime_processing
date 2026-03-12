# Trabalho Final — Simulação Black Friday

## Alunos

Fabio Hemerson Araujo de Souza — Matrícula 2519208

Rebeca Dieb Holanda Silva — Matrícula 2519094

## Visão geral

Este projeto implementa uma simulação de vendas de Black Friday com fluxo ponta a ponta, que utiliza geração de eventos e desenvolve visualização dos indicadores em dashboard.

A solução consome eventos de vendas via Kafka, processa os dados em uma camada intermediária em Python e disponibiliza métricas para visualização em um dashboard construído com Streamlit.

---

## Objetivo

Construir uma solução de dados fim a fim para monitoramento de indicadores de Black Friday, contemplando:

- indicadores C-Level
- performance comercial
- indicadores de controladoria
- atualização contínua a partir de eventos simulados

---

## Arquitetura

Fluxo da solução:

Gerador de eventos → Kafka → Pipeline Python → Arquivos de métricas → Dashboard Streamlit

### Componentes

- Gerador de eventos: scripts disponibilizados na pasta `trabalho/app/src`
- Kafka: responsável pelo transporte dos eventos de pedidos
- Pipeline: camada intermediária criada em `trabalho-final/pipeline`
- Storage: persistência das métricas e histórico em arquivos JSON/JSONL
- Dashboard: visualização dos indicadores em `trabalho-final/dashboard/app.py`

---

## Estrutura do projeto

trabalho/
  app/
    src/
      ... arquivos do gerador e infraestrutura da disciplina disponibilizada pelo professor...

trabalho-final/
  pipeline/
    consumer.py
    transformations.py
    aggregations.py
    storage.py
  dashboard/
    app.py
  data/
    metrics.json

---

## Indicadores implementados

### C-Level
- GMV
- Receita
- Lucro
- Ticket médio
- Pedidos totais
- Pedidos pagos
- Conversão
- Desconto médio

### Performance comercial
- Top vendedores por GMV
- GMV por região
- Top produtos por quantidade vendida
- Top produtos por GMV

### Controladoria
- Taxa de cancelamento
- Taxa de devolução
- Desconto total
- Margem bruta
- Funil de pedidos

---

## Como executar

### 1. Subir a infraestrutura

docker compose up -d

### 2. Criar e ativar ambiente virtual

python3 -m venv venv
source venv/bin/activate

### 3. Instalar dependências

pip install -r trabalho-final/requirements.txt

### 4. Rodar o consumer

python trabalho-final/pipeline/consumer.py

### 5. Rodar o dashboard

streamlit run trabalho-final/dashboard/app.py

---

## Funcionamento do pipeline

O consumer lê eventos do tópico Kafka `blackfriday_orders` em lotes e:

1. transforma os eventos
2. armazena o histórico processado
3. recalcula métricas acumuladas
4. salva os resultados em `metrics.json`

O dashboard consome esse arquivo para exibir os indicadores atualizados.

---

## Tecnologias utilizadas

- Python
- Kafka
- Docker Compose
- Pandas
- Streamlit
- Altair

---

## Resultado

O projeto entrega uma simulação de Black Friday com fluxo completo de dados, processamento analítico e dashboard voltado para acompanhamento executivo, comercial e financeiro.