FROM apache/spark:3.5.7-python3

# pyspark.ml (MLlib) needs numpy; the base image ships Python 3.8
# WITHOUT it, which crashes every ml_training.py run with
# "ModuleNotFoundError: No module named 'numpy'".
# numpy 1.24.4 is the newest release that still supports Python 3.8.
USER root
RUN pip install --no-cache-dir numpy==1.24.4

# ClickHouse JDBC jar for ch_backfill.py, baked at build time so the
# Airflow publish task never needs network at runtime. (--packages rejects
# the ":all" classifier -- that is why we pass this file with --jars.)
RUN mkdir -p /opt/spark-jdbc \
    && python3 -c "import urllib.request; urllib.request.urlretrieve( \
        'https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar', \
        '/opt/spark-jdbc/clickhouse-jdbc-all.jar')"
USER 185
