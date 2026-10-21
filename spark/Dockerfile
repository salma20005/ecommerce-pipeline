# Spark image for the batch + MLlib stages.
#
# Two things the stock apache/spark image is missing:
#   1. numpy -- pyspark.ml imports it, and the official image ships
#      Python 3.8 WITHOUT numpy. 1.24.4 is the newest release that still
#      supports Python 3.8.
#   2. the ClickHouse JDBC jar for ch_backfill.py, baked at build time so
#      the Airflow "publish_to_clickhouse" task never needs network at
#      runtime. (--packages rejects the ":all" classifier -- that is why
#      we pass this file with --jars.)
#
# The laptop runs on a slow/unstable connection, so both downloads retry
# instead of failing the whole build on a single dropped connection.
FROM apache/spark:3.5.7-python3

USER root

# pip defaults to a 15 s read timeout, which is not enough for a 17 MB
# wheel on a shaky line.
ENV PIP_DEFAULT_TIMEOUT=180
RUN pip install --no-cache-dir --retries 10 --timeout 60 numpy==1.24.4

# Download the JDBC jar, retrying up to 5 times before giving up.
RUN mkdir -p /opt/spark-jdbc \
    && python3 - <<'PY' \
       && rm -f /opt/spark-jdbc/*.part
import time
import urllib.request

URL = ("https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/"
       "0.4.6/clickhouse-jdbc-0.4.6-all.jar")
DEST = "/opt/spark-jdbc/clickhouse-jdbc-all.jar"
PART = DEST + ".part"

for attempt in range(1, 6):
    try:
        urllib.request.urlretrieve(URL, PART)
        import os
        os.replace(PART, DEST)
        print("jar downloaded on attempt", attempt)
        break
    except Exception as exc:                      # noqa: BLE001
        print("attempt", attempt, "failed:", exc)
        if attempt == 5:
            raise
        time.sleep(10)
PY

RUN ls -la /opt/spark-jdbc/ && python3 -c "import numpy; print('numpy', numpy.__version__)"

USER 185
