# Black Friday Real-Time Processing

![Arquitetura do Sistema](explanation.png)

Sistema de **simulação e monitoramento de eventos em tempo real**, inspirado em cenários de alta demanda como **Black Friday**.

O projeto tem como objetivo **gerar grandes volumes de dados de vendas**, processá-los continuamente e exibir **métricas operacionais em um dashboard interativo**, permitindo estudar o comportamento de sistemas sob carga.

Ele funciona como um **laboratório de processamento de eventos**, ideal para estudar:

- arquitetura de sistemas
- geração massiva de dados
- processamento em tempo real
- observabilidade
- dashboards operacionais

---

# Visão Geral

O sistema simula um ambiente de **e-commerce sob alta carga**, gerando continuamente eventos de vendas e exibindo métricas operacionais em tempo real.

A plataforma permite:

- gerar dados simulados de vendas
- iniciar e parar geração de eventos
- processar eventos continuamente
- persistir dados em banco
- monitorar métricas em tempo real
- visualizar dados através de um dashboard interativo

As entidades simuladas incluem:

- lojas
- vendedores
- clientes
- pedidos
- canais de venda
- status de pedidos

---

# Arquitetura do Sistema

A arquitetura apresentada na imagem acima representa um **pipeline de processamento distribuído orientado a eventos**, executando dentro de uma rede interna Docker.

Fluxo geral do sistema:

### 1️⃣ Trigger de Importação

Um arquivo CSV ou evento externo inicia o processo de importação via requisição HTTP.

---

### 2️⃣ Django Backend

O Django atua como **interface principal e orquestrador do sistema**.

Responsabilidades:

- Dashboard web
- API REST
- Orquestração de processos
- Persistência de dados
- Integração com serviço de IA / LLM

---

### 3️⃣ Flask Import Service

Serviço responsável por iniciar o pipeline de processamento.

Funções:

- receber requisição de importação
- publicar comandos no Kafka
- iniciar o processamento distribuído

---

### 4️⃣ Apache Kafka

Kafka atua como **backbone de eventos do sistema**.

Gerencia tópicos como:

- `import.commands`
- `import.data`

Ele permite:

- desacoplamento entre serviços
- processamento assíncrono
- escalabilidade horizontal

---

### 5️⃣ Consumers de Processamento

Serviços consumidores processam os eventos do Kafka.

Responsabilidades:

- consumir dados do streaming
- processar registros
- distribuir carga de processamento

---

### 6️⃣ Redis

Redis funciona como **camada de estado e cache do sistema**.

Armazena:

- progresso da importação
- status do processamento
- contadores de eventos

Isso permite atualização **em tempo real do dashboard**.

---

### 7️⃣ PostgreSQL

Banco de dados relacional final do sistema.

Responsável por armazenar:

- dados processados
- registros consolidados
- informações históricas

---

### 8️⃣ Serviço de IA / LLM

Serviço responsável por gerar **insights automáticos com base nos dados processados**.

Exemplo de uso:

- geração de relatórios
- análise automática
- insights inteligentes

---

### 9️⃣ Dashboard

Interface visual do sistema que mostra:

- progresso em tempo real
- métricas operacionais
- relatórios
- status do processamento

---

# Tecnologias Utilizadas

## Backend

- Python
- Django
- Flask

## Streaming e Mensageria

- Apache Kafka

## Persistência

- PostgreSQL
- Redis

## Infraestrutura

- Docker
- Docker Compose

## Frontend

- HTML
- TailwindCSS
- JavaScript

---

# Estrutura do Projeto

Exemplo simplificado:
project/

├─ dashboard/
│ ├─ templates/
│ └─ views.py

├─ generator/
│ ├─ services/
│ │ └─ generator_client.py
│ └─ views.py

├─ orders/
│ ├─ models.py
│ └─ services/

├─ core/

├─ docker/

├─ requirements.txt

└─ README.md


---

# Executando o Projeto

Para iniciar o sistema basta subir os containers:

```bash
docker compose up --build

Durante a inicialização dos containers o sistema executa automaticamente:

inicialização do banco de dados

migrations do Django

inicialização da aplicação

Acessando o Dashboard

Abra no navegador:

http://localhost:8000
Utilizando o Sistema

Acesse o dashboard

Clique em Gerar Dados

O sistema começará a gerar eventos automaticamente

O dashboard atualizará as métricas

Para interromper o processo clique em Parar Geração

Possíveis Evoluções

O projeto pode evoluir para incluir:

Kafka Streams

WebSockets para atualização em tempo real

Prometheus para métricas

Grafana para observabilidade

workers assíncronos

múltiplos produtores de eventos

balanceamento de carga

Objetivo do Projeto

Este projeto foi desenvolvido como um laboratório para estudo de sistemas distribuídos e processamento de dados em larga escala, simulando cenários de alta demanda como eventos de Black Friday.

Ele permite explorar conceitos importantes de:

arquitetura de microsserviços

processamento orientado a eventos

pipelines de dados

observabilidade

dashboards operacionais