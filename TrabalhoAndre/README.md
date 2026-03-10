# Realtime Processing – Black Friday Dashboard

## 📌 Descrição do Projeto

Este projeto foi desenvolvido como parte de um trabalho de pós-graduação e tem como objetivo **simular um pipeline de processamento de dados em tempo real durante uma promoção de Black Friday**.

O sistema gera dados simulados de pedidos, processa esses dados em diferentes camadas e exibe os resultados em um **dashboard atualizado automaticamente**, permitindo acompanhar métricas de vendas e performance.

O projeto simula um fluxo simplificado de **engenharia de dados e analytics em tempo real**.

---

# 🧱 Arquitetura do Projeto

O fluxo do projeto segue a seguinte estrutura:

```
Geração de Dados (RAW)
        ↓
Tratamento e Enriquecimento (SILVER)
        ↓
Dashboard em Tempo Real
```

---

# 🛠 Tecnologias Utilizadas

* Python
* Pandas
* Streamlit
* Streamlit Auto Refresh

---

# 📂 Estrutura do Projeto

```
TrabalhoAndre
│
├── dados_raw
│   ├── orders_seed.csv
│   └── sellers_targets.csv
│
├── dados_silver
│   ├── orders_silver.csv
│   └── sellers_silver.csv
│
├── generate_seed_data.py
├── process_to_silver.py
└── stream_app.py
```

---

# ⚙️ Pipeline de Dados

## 1️⃣ Geração de Dados (RAW)

O script **generate_seed_data.py** cria dados simulados de vendas da Black Friday.

São gerados:

* **20.000 pedidos simulados**
* **10 vendedores**
* informações de:

  * produtos
  * região
  * canal de venda (app/web)
  * descontos
  * método de pagamento
  * campanhas de marketing

Os dados são armazenados na pasta:

```
dados_raw/
```

Arquivos gerados:

* `orders_seed.csv`
* `sellers_targets.csv`

---

# 2️⃣ Processamento e Enriquecimento (SILVER)

O script **process_to_silver.py** realiza o tratamento dos dados.

Transformações realizadas:

* conversão de datas
* criação de **flag de cancelamento**
* cálculo de **margem**
* cálculo de **ticket médio**
* cálculo de **receita válida (ignorando pedidos cancelados)**
* cálculo de **margem válida**

Também ocorre **renomeação de colunas para padronização analítica**.

Os dados processados são salvos na camada:

```
dados_silver/
```

Arquivos gerados:

* `orders_silver.csv`
* `sellers_silver.csv`

---

# 3️⃣ Dashboard em Tempo Real

O script **stream_app.py** cria um dashboard interativo usando **Streamlit**.

O sistema simula um fluxo de dados em tempo real utilizando:

* leitura dos dados em **chunks**
* atualização automática a cada **5 segundos**

Isso cria a sensação de um **streaming de pedidos acontecendo durante a Black Friday**.

---

# 📊 Métricas Exibidas no Dashboard

O dashboard apresenta diferentes camadas de análise:

### 🔴 C-Level

* GMV (Receita total)
* Lucro líquido
* Ticket médio

### 🔵 Vendas

* GMV por vendedor
* Top 5 vendedores
* Bottom 5 vendedores
* Número total de pedidos
* Meta vs realizado por vendedor

### 🟢 Controladoria

* Margem bruta
* Custo total
* Impacto dos descontos
* Taxa de cancelamento
* Lucro por produto

### 🟡 Insights adicionais

* Receita por região
* Produtos mais cancelados
* Receita acumulada ao longo do tempo

---

# ▶️ Como Executar o Projeto

### 1️⃣ Gerar os dados simulados

```
python generate_seed_data.py
```

---

### 2️⃣ Processar os dados

```
python process_to_silver.py
```

---

### 3️⃣ Iniciar o Dashboard

```
streamlit run stream_app.py
```

---

# 📈 Resultado Esperado

Ao executar o projeto, será exibido um **dashboard interativo que simula um monitoramento em tempo real de vendas da Black Friday**.

O sistema adiciona novos dados periodicamente e atualiza as métricas automaticamente a cada **5 segundos**.

---

# 👨‍💻 Autor

Andre Parente
Projeto desenvolvido como trabalho de pós-graduação.
