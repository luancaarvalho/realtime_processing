
<h1 align="center">
  Demo05 — Análise e Visualização (Desafio)
</h1>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.8%2B-blue">
  <img alt="Kafka" src="https://img.shields.io/badge/Kafka-Local-orange">
  <img alt="Plotly" src="https://img.shields.io/badge/Plotly-Interactive-purple">
</p>

<br>

## 💻 Resumo do projeto

Este diretório contém os scripts utilizados no Demo05: um produtor Kafka que gera eventos de teste, um consumidor que grava os eventos em um arquivo JSON e scripts para gerar visualizações dos dados. A versão final do gráfico é produzida com `plotly` e inclui a identidade visual da Unifor (logo e cabeçalho). O output inclui uma versão interativa em HTML e uma imagem PNG pronta para submissão.

<br>

## ✨ Tecnologias

- Linguagem: **Python 3.8+**
- Mensageria: **Kafka** (ex.: via `docker compose` no ambiente local)
- Visualização: **Plotly** (interativa) e **Matplotlib / Seaborn** (estática)
- Export estático: **Kaleido** (usado por Plotly para exportar PNG/SVG)

<br>

## 📁 Estrutura dos arquivos relevantes

```text
aula01/demo05/
├── demo5_producer.py             # produtor Kafka (gera N eventos)
├── demo5_consumer_exercise.py    # consumidor que grava demo5_data.json
├── demo5_data.json               # dados gerados pelo consumidor (entrada para os plots)
├── desafio_demo05_plotly.py      # script Plotly para HTML + PNG (usa logo)
├── plot_demo5.py                 # script alternativo com matplotlib/seaborn
└── unifor_logo.png               # logo usada no cabeçalho do gráfico
```

<br>

## 🚀 Como executar (passo a passo)

Siga os passos abaixo para replicar o experimento e gerar os gráficos.

### 1. Preparar o ambiente Python

```bash
# criar e ativar venv (recomendado)
python3 -m venv .venv
source .venv/bin/activate

# instalar dependências
pip install -r aula01/requirements.txt
```

> Observação: se preferir instalar pacotes pontuais, garanta `confluent-kafka`, `orjson`, `pandas`, `plotly`, `kaleido`, `seaborn` e `matplotlib`.

### 2. Subir infraestrutura (opcional — via Docker Compose)

```bash
docker compose up -d
```

Se ocorrer erro por porta ocupada (ex.: `5432`), verifique o processo que está usando a porta e pare-o ou altere o `docker-compose.yml`.

```bash
sudo lsof -nP -iTCP:5432 -sTCP:LISTEN
```

### 3. Produzir eventos (exemplo: 50 registros)

```bash
python3 aula01/demo05/demo5_producer.py --bootstrap localhost:9092 --topic rt.events.etpt --records 50
```

### 4. Consumir eventos e gravar JSON

```bash
python3 aula01/demo05/demo5_consumer_exercise.py --bootstrap localhost:9092 --topic rt.events.etpt --records 50 --output aula01/demo05/demo5_data.json
```

### 5. Gerar o gráfico final (Plotly)

```bash
python3 aula01/demo05/desafio_demo05_plotly.py \
  --input aula01/demo05/demo5_data.json \
  --output-html aula01/demo05/desafio_demo05_plot.html \
  --output-png aula01/demo05/desafio_demo05_plot.png \
  --logo aula01/demo05/unifor_logo.png
```

Alternativa: gerar a versão estática com Matplotlib/Seaborn

```bash
python3 aula01/demo05/plot_demo5.py --input aula01/demo05/demo5_data.json --output aula01/demo05/demo5_plot.png
```

<br>

## 📊 Entregáveis

- `aula01/demo05/demo5_data.json` — arquivo de dados gerado pelo consumidor.
- `aula01/demo05/desafio_demo05_plot.html` — visualização interativa (Plotly).
- `aula01/demo05/desafio_demo05_plot.png` — imagem PNG para submissão.

<br>

## 🛠️ Resolução de problemas comuns

- `ModuleNotFoundError: No module named 'orjson'` — execute `pip install orjson` ou instale todas as dependências com o `requirements.txt`.
- Erro ao exportar imagem com Plotly — instale `kaleido` (`pip install kaleido`).
- `docker compose` falha por porta ocupada — pare o serviço local que usa a porta, ou altere o mapeamento no `docker-compose.yml`.

<br>

## 🎨 Personalização e ajustes visuais

O script `desafio_demo05_plotly.py` contém parâmetros de layout (margens, posição do logo, fontes). Para ajustar o cabeçalho (posicionamento do logo/títulos) edite as variáveis de layout no arquivo e reexecute o comando de geração.

Se desejar, posso aplicar ajustes específicos: alinhar logo, alterar fontes, incluir nome do aluno e matrícula no cabeçalho.

<br>

## 📄 Licença / Autoria

Material desenvolvido para submissão do trabalho de aula por **[rafaeld3v](https://www.linkedin.com/in/rafaeld3v/)**.

