# Projeto Black Friday Real-Time Data Pipeline

Este projeto simula uma pipeline de dados em tempo real para um cenário de Black Friday, incluindo geração de eventos, processamento com Apache Spark e visualização em um dashboard Streamlit.

## Estrutura do Projeto

O projeto está dividido em dois arquivos `docker-compose` para facilitar a gestão dos serviços:

1.  `docker-compose.infra.yml`: Contém os serviços de infraestrutura como Kafka, Zookeeper, MySQL, MinIO e Kafka UI.
2.  `docker-compose.pipeline.yml`: Contém os serviços da pipeline de dados, incluindo o gerador de seed, o bootstrap de dados, o gerador de stream, o processador Spark e o dashboard Streamlit.

## Pré-requisitos

*   Docker Desktop ou Docker Engine instalado e rodando.
*   Conexão com a internet para download das imagens Docker e dependências do Spark.

## Como Rodar o Projeto

Siga os passos abaixo para iniciar e executar a pipeline completa.

### 1. Construir as Imagens Docker (Primeira Vez ou Após Alterações no `Dockerfile`/`requirements.txt`)

É crucial construir as imagens para garantir que todas as dependências Python (incluindo PySpark) estejam instaladas corretamente.

```bash
docker-compose -f docker-compose.infra.yml build
docker-compose -f docker-compose.pipeline.yml build --no-cache
```

O `--no-cache` no segundo comando garante que as dependências do PySpark sejam instaladas novamente, caso o `requirements.txt` tenha sido atualizado.

### 2. Iniciar os Serviços de Infraestrutura

Primeiro, suba os serviços de infraestrutura. Isso pode levar alguns minutos na primeira vez, pois as imagens serão baixadas.

```bash
docker-compose -f docker-compose.infra.yml up -d
```

Verifique se todos os serviços estão rodando:

```bash
docker-compose -f docker-compose.infra.yml ps
```

Você pode acessar a Kafka UI em `http://localhost:8080` para monitorar os tópicos do Kafka.

### 3. Iniciar a Pipeline de Dados

Após a infraestrutura estar de pé, inicie os serviços da pipeline.

```bash
docker-compose -f docker-compose.pipeline.yml up -d
```

**Observação Importante para o `bf-processor` (Spark):**
Na primeira execução do `bf-processor`, o Spark fará o download de JARs adicionais (como `aws-java-sdk-bundle`) da internet. Este download é grande e pode levar vários minutos. **Não interrompa o processo** (ex: com `Ctrl+C`) durante esta fase. Você pode monitorar o progresso nos logs:

```bash
docker logs -f bf-processor
```

Aguarde até que o log do `bf-processor` comece a mostrar mensagens como "Processing batch..." ou "Writing raw batch to MinIO...".

### 4. Acessar o Dashboard

Uma vez que o `bf-processor` esteja rodando e processando dados, o dashboard Streamlit estará disponível.

*   **Dashboard de Vendas:** `http://localhost:8505`

### 5. Parar os Serviços

Para parar todos os serviços e remover os containers:

```bash
docker-compose -f docker-compose.pipeline.yml down
docker-compose -f docker-compose.infra.yml down
```

Para remover também os volumes (dados do MinIO, por exemplo):

```bash
docker-compose -f docker-compose.pipeline.yml down -v
docker-compose -f docker-compose.infra.yml down -v
```

### Solução de Problemas Comuns

*   **`ModuleNotFoundError: No module named 'pyspark'`**: Isso geralmente significa que a imagem Docker não foi reconstruída corretamente. Certifique-se de ter executado `docker-compose -f docker-compose.pipeline.yml build --no-cache` e tentado novamente.
*   **Serviços não iniciam ou travam**: Verifique os logs de cada serviço individualmente. Por exemplo, para o MySQL: `docker logs mysql`. Para o Spark: `docker logs bf-processor`.
*   **`bf-processor` demora muito para iniciar**: Como mencionado, o download inicial dos JARs do Spark pode ser demorado. Tenha paciência na primeira vez.
*   **`kafka_net` network not found**: Certifique-se de que a rede `kafka_net` foi criada. O `docker-compose.infra.yml` deve criar essa rede automaticamente. Se você parou os serviços com `down -v`, a rede pode ter sido removida e precisará ser recriada.

---

Com esta divisão, você tem mais controle sobre quais partes da sua infraestrutura e pipeline estão ativas, o que é útil para desenvolvimento e depuração.
