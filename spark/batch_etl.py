"""
batch_etl.py
============
Stage 3 of the pipeline: Spark Batch ETL.

What this job does (the "clean + enrich" step):
  1. Reads the raw 2019-Oct.csv from the mounted /data volume.
  2. Cleans it: fixes types, drops nulls / duplicates / invalid prices.
  3. Engineers features: hour, weekday, top-level category...
  4. Aggregates to session level (one row per user_session) - this is
     the dataset the ML model will be trained on later.
  5. Writes everything to HDFS as partitioned Parquet:
        /raw/2019-Oct            (untouched copy, kept for lineage)
        /processed/events_clean  (cleaned event-level data)
        /curated/sessions        (session-level training dataset)

Usage (run inside the "spark" container):
    docker exec spark /opt/spark/bin/spark-submit \
        --master 'local[2]' \
        --driver-memory 4g \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-jobs/batch_etl.py

Why local[2] and 4g: this runs on a laptop with 20 GB RAM shared with
other containers, so we deliberately stay small. On a real cluster you
would ask for far more.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

# --- config -----------------------------------------------------------------

# NOTE: file:// prefix is required - without it Spark resolves "/" against
# fs.defaultFS (HDFS) and looks for /data on the datanodes, not in the container.
INPUT_CSV = "file:///data/2019-Oct.csv"

# "limit" lets us smoke-test on a slice before running the full 5 GB file.
# spark.read.csv has no row limit, so we cap by taking N partitions instead
# of parsing everything twice; for the smoke test we just read a sample.
SMOKE_TEST = "--smoke" in sys.argv

HDFS_RAW = "hdfs://namenode:9000/raw/2019-Oct"
HDFS_PROCESSED = "hdfs://namenode:9000/processed/events_clean"
HDFS_CURATED = "hdfs://namenode:9000/curated/sessions"

# Columns that must never be null for an event to be usable.
REQUIRED_COLS = ["event_time", "event_type", "product_id", "user_id", "user_session"]


def build_spark(app_name: str) -> SparkSession:
    """Create a local SparkSession wired to our single-node HDFS."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "8")   # laptop-sized, not cluster-sized
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession):
    """Read the CSV with an explicit schema - no inferSchema on 5 GB."""
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),   # read as string, cast later
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ])

    reader = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")          # bad rows -> nulls, not a crash
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        .schema(schema)
        .csv(INPUT_CSV)
    )

    if SMOKE_TEST:
        # Only pull a few partitions so the run takes ~1 minute, not ~30.
        reader = reader.limit(200_000)

    return reader


def clean(df):
    """Drop junk rows and cast everything to its real type."""
    df = df.dropDuplicates()  # exact duplicate rows

    # Cast types
    df = (
        df
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("product_id", F.col("product_id").cast(LongType()))
        .withColumn("category_id", F.col("category_id").cast(DoubleType()))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("user_id", F.col("user_id").cast(LongType()))
    )

    # Drop rows missing anything we cannot recover
    df = df.dropna(subset=REQUIRED_COLS)

    # Business rule: a price must be > 0 for a purchase to make sense
    df = df.filter(F.col("price") > 0)

    # Only the three event types we care about
    df = df.filter(F.col("event_type").isin(["view", "cart", "purchase"]))

    return df


def engineer(df):
    """Add derived columns the rest of the pipeline will use."""
    return (
        df
        # Top-level category, e.g. "electronics" from "electronics.tablet"
        .withColumn("category_top", F.split(F.col("category_code"), r"\.")[0])
        .withColumn("hour", F.hour("event_time"))
        .withColumn("weekday", F.dayofweek("event_time"))       # 1 = Sunday
        .withColumn("date", F.to_date("event_time"))
    )


def aggregate_sessions(df):
    """Collapse every event into ONE row per user_session.

    This is what the ML model will eventually train on: for each session,
    did the shopper buy (label=1) or not (label=0)?
    """
    return (
        df.groupBy("user_session", "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum((F.col("event_type") == "view").cast("int")).alias("view_count"),
            F.sum((F.col("event_type") == "cart").cast("int")).alias("cart_count"),
            F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchase_count"),
            F.count_distinct("product_id").alias("unique_products"),
            F.sum("price").alias("total_price"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
        )
        # A session counts as "purchased" if it contains any purchase event
        .withColumn(
            "purchased",
            (F.col("purchase_count") > 0).cast(IntegerType()),
        )
        # Session length in seconds
        .withColumn(
            "session_length_sec",
            (F.col("session_end").cast("long") - F.col("session_start").cast("long")),
        )
        .drop("session_start", "session_end")
    )


def main() -> None:
    spark = build_spark("ecommerce-batch-etl")

    print(f"[batch_etl] mode = {'SMOKE TEST (200k rows)' if SMOKE_TEST else 'FULL RUN'}", flush=True)

    # 1. read ---------------------------------------------------------------
    df = read_raw(spark)
    print(f"[batch_etl] read raw csv, partitions = {df.rdd.getNumPartitions()}", flush=True)

    # 2. clean --------------------------------------------------------------
    clean_df = clean(df)
    clean_df = engineer(clean_df)

    # cache because we use it three times (raw copy, events, sessions)
    clean_df.cache()
    clean_count = clean_df.count()
    print(f"[batch_etl] clean rows = {clean_count:,}", flush=True)

    # 3. write /raw (lineage copy, so we can always re-derive everything) ---
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(HDFS_RAW)
    )
    print(f"[batch_etl] wrote {HDFS_RAW}", flush=True)

    # 4. write /processed (event level, for the dashboards) ----------------
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(HDFS_PROCESSED)
    )
    print(f"[batch_etl] wrote {HDFS_PROCESSED}", flush=True)

    # 5. build + write /curated (session level, for ML) --------------------
    sessions = aggregate_sessions(clean_df)
    (
        sessions.write
        .mode("overwrite")
        .parquet(HDFS_CURATED)
    )
    print(f"[batch_etl] wrote {HDFS_CURATED}", flush=True)

    # 6. quick sanity numbers so the log is useful --------------------------
    total_sessions = sessions.count()
    buyers = sessions.filter(F.col("purchased") == 1).count()
    print(f"[batch_etl] sessions = {total_sessions:,}", flush=True)
    print(f"[batch_etl] buyers   = {buyers:,}", flush=True)
    if total_sessions:
        print(f"[batch_etl] conversion rate = {buyers / total_sessions:.4%}", flush=True)

    spark.stop()
    print("[batch_etl] done.", flush=True)


if __name__ == "__main__":
    main()
