"""
replay_producer.py
==================
Stage 1 of the pipeline: Kafka Ingestion.

Reads the RE46 eCommerce CSV (2019-Oct.csv) in chunks -- to control memory
on a laptop -- and pushes every row as a JSON message to a Kafka topic.

Key design points (you will be asked about these):
  * Message key = user_session  -> Kafka guarantees all events of one
    session land in the SAME partition, so their order is preserved.
  * Chunked reads (pandas)     -> never load 5 GB into RAM at once.
  * Timestamps preserved       -> event_time is carried through untouched.
"""

import json
import os
import sys
import time

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CSV_PATH = os.getenv("CSV_PATH", "/data/2019-Oct.csv")
TOPIC = os.getenv("TOPIC", "user-events")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "2000"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))


def wait_for_kafka(bootstrap: str, timeout: int = 120):
    """Kafka may still be starting when the producer container comes up."""
    from kafka.errors import NoBrokersAvailable

    start = time.time()
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks=1,
                linger_ms=20,
                batch_size=32 * 1024,
            )
        except NoBrokersAvailable:
            if time.time() - start > timeout:
                raise
            print("waiting for kafka...", flush=True)
            time.sleep(5)


def main() -> None:
    if not os.path.exists(CSV_PATH):
        sys.exit(f"CSV not found: {CSV_PATH}")

    producer = wait_for_kafka(BOOTSTRAP)
    print(f"connected to {BOOTSTRAP}, replaying {CSV_PATH} -> {TOPIC}", flush=True)

    delay = 1.0 / EVENTS_PER_SEC if EVENTS_PER_SEC > 0 else 0
    sent = 0
    start = time.time()

    # chunked read: memory stays flat no matter how big the file is
    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        rows = chunk.where(pd.notnull(chunk), None).to_dict(orient="records")
        sent_skipped = 0
        for row in rows:
            # some CSV rows have an empty user_session (NaN -> None); the key
            # serializer would crash on them. Invalid sessions are dropped
            # anyway by batch_etl/streaming (dropna), so skip them at the
            # source too -- "validate before processing".
            if row.get("user_session") is None:
                sent_skipped += 1
                continue
            producer.send(TOPIC, key=row["user_session"], value=row)
            sent += 1
            if delay:
                time.sleep(delay)
        if sent_skipped:
            print(f"skipped {sent_skipped} rows with null user_session", flush=True)

        elapsed = time.time() - start
        rate = sent / elapsed if elapsed else 0
        print(f"sent {sent:,} events ({rate:,.0f} events/sec)", flush=True)

    producer.flush()
    producer.close()
    print(f"done. total events sent: {sent:,}", flush=True)


if __name__ == "__main__":
    main()
