# 🛒 Real-Time Black Friday Sales Simulation

Pipeline de **streaming de dados em tempo real** que simula vendas de Black Friday e processa eventos utilizando **Apache Kafka**, **Python**, **DuckDB** e **Streamlit**.

O projeto gera eventos de vendas, envia para Kafka, consome esses eventos e armazena em DuckDB para análise e visualização.

---

# Arquitetura do Projeto

```
Sales Generator (Python)
        │
        ▼
Apache Kafka (Streaming)
        │
        ▼
Consumer (Python)
        │
        ▼
DuckDB (Armazenamento analítico)
        │
        ▼
Streamlit Dashboard (Visualização)
```

# Tecnologias Utilizadas

* Python
* Apache Kafka
* DuckDB
* Polars / Pandas
* Streamlit

---

# Estrutura do Projeto

```
simular-black-friday/
│
├── generator/
│   └── producer.py
│
├── streaming/
│   └── consumer.py
|   └──database/
|       └── black_friday.duckdb
│
├── dashboard/
│   └── app.py
│
├── requirements.txt
└── README.md
```

---

# Pré-requisitos


* Python 3.10+
* Apache Kafka
* Java (necessário para Kafka)

---

# Instalação

Clone o repositório:

```bash
git clone https://github.com/seu-usuario/black-friday-streaming.git
cd black-friday-streaming
```

Crie e ative o ambiente virtual:

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux / Mac:

```bash
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

#  Criando o Tópico Kafka

Crie o tópico que receberá os eventos de vendas:

```bash
bin\windows\kafka-topics.bat --create --topic black_friday --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verifique se o tópico foi criado:

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
---

#  Executando o Pipeline

### 1. Iniciar o Consumer

```bash
python streaming/consumer.py
---

### 2. Iniciar o Producer (Gerador de vendas)

Em outro terminal:

```bash
python generator/producer.py
```
---

# Dashboard em Tempo Real

Execute o dashboard:

```bash
streamlit run dashboard/app.py
```
