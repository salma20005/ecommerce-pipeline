FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
# slow/unstable connection: give pip long reads and many retries
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 20 --timeout 120 -r requirements.txt

COPY replay_producer.py .

CMD ["python", "replay_producer.py"]
