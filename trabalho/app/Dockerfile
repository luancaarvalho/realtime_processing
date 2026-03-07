FROM python:3.11-slim

# Install Java (required for PySpark) and system utilities
RUN apt-get update && \
    apt-get install -y default-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Set PYTHONPATH to include src so imports work correctly
ENV PYTHONPATH=/app/src

CMD ["python", "src/stream_processor.py"]
