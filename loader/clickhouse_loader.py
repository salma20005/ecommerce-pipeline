"""
clickhouse_loader.py
====================
Stage 4 of the pipeline: the analytical serving layer (ClickHouse).

Consumes the two SUMMARY topics that spark/streaming_etl.py produces
    event-metrics   -> ecommerce.event_metrics
    business-alerts -> ecommerce.business_alerts
and inserts every message into ClickHouse over its HTTP interface.

Design points (you will be asked about these):
  * Starts at the earliest offset ("auto_offset_reset=earliest"), so it
    also BACKFILLS everything the streaming job already produced.
  * ReplacingMergeTree on the ClickHouse side absorbs duplicates: the
    streaming sink runs in "update" mode, so a window can arrive twice.
  * Only the Python standard library talks to ClickHouse (urllib) --
    no extra dependency, the container stays tiny.
  * Retries instead of crashing: ClickHouse may still be starting up.

Run (as its own container via docker compose):
    docker compose up -d ch-loader
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CH_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")
GROUP_ID = os.getenv("GROUP_ID", "ch-loader")

# Kafka topic -> ClickHouse table
TOPIC_TO_TABLE = {
    "event-metrics": "event_metrics",
    "business-alerts": "business_alerts",
}

FLUSH_ROWS = 500       # insert when this many rows are buffered...
FLUSH_SECONDS = 5      # ...or when this many seconds passed


def to_clickhouse_time(value):
    """Spark's to_json writes timestamps as '2019-10-02T15:30:00.000Z';
    ClickHouse wants '2019-10-02 15:30:00'."""
    if not isinstance(value, str):
        return value
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def prepare_row(topic, payload):
    """Map a Kafka JSON message to the columns of its ClickHouse table."""
    if topic in ("event-metrics", "business-alerts"):
        for col in ("window_start", "window_end"):
            if col in payload:
                payload[col] = to_clickhouse_time(payload[col])
    return payload


def insert_rows(table, rows):
    """POST rows as JSONEachRow; retry a few times before giving up."""
    body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
    url = f"{CH_URL}/?query=" + urllib.parse.quote(
        f"INSERT INTO {CH_DB}.{table} FORMAT JSONEachRow"
    )
    for attempt in range(1, 11):
        try:
            req = urllib.request.Request(url, data=body)
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp.read()
            return True
        except (urllib.error.URLError, TimeoutError) as exc:
            print(f"[ch-loader] insert into {table} failed "
                  f"(attempt {attempt}/10): {exc}", flush=True)
            time.sleep(min(attempt * 2, 10))
    print(f"[ch-loader] giving up on batch for {table}", flush=True)
    return False


def wait_for_clickhouse(timeout: int = 300) -> None:
    start = time.time()
    while True:
        try:
            with urllib.request.urlopen(f"{CH_URL}/ping", timeout=5) as r:
                if r.status == 200:
                    return
        except OSError:
            pass
        if time.time() - start > timeout:
            raise TimeoutError("ClickHouse did not answer /ping in time")
        print("[ch-loader] waiting for ClickHouse...", flush=True)
        time.sleep(5)


def wait_for_kafka(bootstrap: str, timeout: int = 180):
    from kafka.errors import NoBrokersAvailable

    start = time.time()
    while True:
        try:
            return KafkaConsumer(
                *TOPIC_TO_TABLE,
                bootstrap_servers=bootstrap,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",   # backfill + live tail
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            if time.time() - start > timeout:
                raise
            print("[ch-loader] waiting for Kafka...", flush=True)
            time.sleep(5)


def main() -> None:
    wait_for_clickhouse()
    consumer = wait_for_kafka(BOOTSTRAP)
    print(f"[ch-loader] consuming {list(TOPIC_TO_TABLE)} -> "
          f"{CH_URL}/{CH_DB}", flush=True)

    buffers = {topic: [] for topic in TOPIC_TO_TABLE}
    totals = {topic: 0 for topic in TOPIC_TO_TABLE}
    last_flush = time.time()

    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=200)
        for _tp, messages in polled.items():
            topic = _tp.topic
            for msg in messages:
                try:
                    payload = msg.value
                except Exception as exc:      # bad JSON: log & skip, keep pipeline alive
                    print(f"[ch-loader] bad message skipped: {exc}", flush=True)
                    continue
                buffers[topic].append(prepare_row(topic, payload))

        now = time.time()
        for topic, rows in buffers.items():
            if not rows:
                continue
            if len(rows) >= FLUSH_ROWS or now - last_flush >= FLUSH_SECONDS:
                if insert_rows(TOPIC_TO_TABLE[topic], rows):
                    totals[topic] += len(rows)
                    print(f"[ch-loader] {topic}: {len(rows)} rows -> "
                          f"{TOPIC_TO_TABLE[topic]} (total {totals[topic]:,})",
                          flush=True)
                buffers[topic] = []
        if now - last_flush >= FLUSH_SECONDS:
            last_flush = now


if __name__ == "__main__":
    import urllib.parse  # noqa: E402  (used inside insert_rows)
    main()
