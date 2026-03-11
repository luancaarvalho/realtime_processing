FROM python:3.11-slim

WORKDIR /app

# Dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código
COPY setup_db.py generator.py dashboard.py ./

# Volume compartilhado para o banco SQLite
RUN mkdir -p /app/data

ENV DB_PATH=/app/data/blackfriday.db
