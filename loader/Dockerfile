FROM python:3.11-slim

WORKDIR /app

ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 20 --timeout 120 kafka-python==2.0.2

COPY clickhouse_loader.py .

CMD ["python", "clickhouse_loader.py"]
