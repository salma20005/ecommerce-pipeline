"""
ch_backfill.py
==============
Stage 4 of the pipeline: one-off backfill of the ClickHouse serving layer.

The batch/funnel jobs already wrote their results as PARQUET in HDFS.
This job pushes the three small aggregate tables into ClickHouse using
the JDBC writer (the poster's "Spark writes results into it (JDBC)"):

    /curated/funnel_daily  ->  ecommerce.daily_funnel
    /curated/revenue_daily ->  ecommerce.daily_revenue
    /curated/funnel        ->  ecommerce.funnel_overall

The live tables (event_metrics / business_alerts) are filled by
loader/clickhouse_loader.py straight from Kafka - no backfill needed.

Run AFTER ClickHouse is up and clickhouse/init.sql has been applied.
Note: --packages rejects the ":all" classifier, so the shaded jar is
downloaded once and passed with --jars instead:

    docker exec spark python3 -c "import urllib.request; \
        urllib.request.urlretrieve( \
        'https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar', \
        '/tmp/clickhouse-jdbc-all.jar')"

    docker exec spark /opt/spark/bin/spark-submit \
        --master 'local[2]' \
        --driver-memory 1g \
        --jars /tmp/clickhouse-jdbc-all.jar \
        /opt/spark-jobs/ch_backfill.py
"""

from pyspark.sql import SparkSession

CH_URL = "jdbc:clickhouse://clickhouse:8123/ecommerce"
CH_PROPS = {
    "user": "default",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

# (HDFS parquet path, ClickHouse table)
TABLES = [
    ("hdfs://namenode:9000/curated/funnel_daily", "daily_funnel"),
    ("hdfs://namenode:9000/curated/revenue_daily", "daily_revenue"),
    ("hdfs://namenode:9000/curated/funnel", "funnel_overall"),
]


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("clickhouse-backfill")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

    for path, table in TABLES:
        df = spark.read.parquet(path)
        count = df.count()
        # keyword args on purpose: in Spark 3.5 the signature is
        # jdbc(url, table, mode, properties) - passing CH_PROPS positionally
        # would land it in the "mode" parameter.
        df.write.jdbc(
            url=CH_URL,
            table=f"ecommerce.{table}",
            mode="append",          # append, not overwrite: overwrite would DROP
                                    # the table and lose the ReplacingMergeTree
                                    # engine defined in init.sql.
            properties=CH_PROPS,
        )
        print(f"[ch-backfill] {table}: wrote {count:,} rows", flush=True)

    spark.stop()
    print("[ch-backfill] done.", flush=True)


if __name__ == "__main__":
    main()
