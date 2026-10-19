"""
streaming_etl.py
================
Stage 3B of the pipeline: Spark Structured Streaming.

Reads the live clickstream from Kafka topic "user-events" and, every
5 minutes of event-time, produces:
  * windowed counts        (views / carts / purchases)
  * rolling conversion rate (purchases / views)
  * anomaly alerts         -> written to Kafka topic "business-alerts"
  * processed summaries    -> written to Kafka topic "event-metrics"

Why streaming and not batch?
  Batch answers "what happened in October 2019".
  Streaming answers "what is happening right now" -- this is what makes
  the pipeline a real-time system instead of a report generator.

Run:
    docker exec -it spark /opt/spark/bin/spark-submit \
        --master 'local[2]' \
        --driver-memory 3g \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
        /opt/spark-jobs/streaming_etl.py

The "--packages" line downloads the Kafka connector the first time only.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- config -----------------------------------------------------------------

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_IN = "user-events"          # raw events from the replay producer
TOPIC_METRICS = "event-metrics"   # windowed summaries (Power BI reads these)
TOPIC_ALERTS = "business-alerts"  # anomaly notifications

WINDOW_MINUTES = 5                # required by the poster
CHECKPOINT = "file:///data/checkpoints/streaming"   # survives restarts

# Anomaly rule: alert when a window's conversion rate jumps far above the
# recent baseline. Simple, explainable, no ML needed for this step.
ALERT_FACTOR = 3.0        # 3x the mean
ALERT_MIN_VIEWS = 200     # ignore tiny windows, too noisy

EVENT_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", DoubleType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


def read_from_kafka(spark: SparkSession):
    """Continuous stream of raw JSON events from Kafka."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")   # only new events, not replay history
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka gives us: key (bytes), value (bytes), topic, partition, timestamp.
    # We only need the JSON payload we produced earlier.
    return (
        raw
        .withColumn("key", F.col("key").cast(StringType()))
        .withColumn(
            "payload",
            F.from_json(F.col("value").cast(StringType()), EVENT_SCHEMA),
        )
        .select("key", "payload.*")
        .dropna(subset=["event_time", "event_type", "user_session"])
    )


def aggregate_windows(df):
    """One row per 5-minute event-time window with funnel counts."""
    return (
        df
        .withWatermark("event_time", "10 minutes")   # tolerate late data
        .groupBy(F.window(F.col("event_time"), f"{WINDOW_MINUTES} minutes"))
        .agg(
            F.count("*").alias("event_count"),
            # approx_count_distinct: streaming does not support count(DISTINCT ...);
            # HyperLogLog estimate (~96-99% accurate) is fine for a metrics dashboard.
            F.approx_count_distinct("user_session").alias("sessions"),
            F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
            F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"),
            F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)
            ).alias("revenue"),
        )
        # conversion rate = purchases / views (guard against divide-by-zero)
        .withColumn(
            "conversion_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(
                F.lit(0.0)
            ).cast(DoubleType()),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def detect_anomalies(metrics_df):
    """Flag windows whose conversion rate spikes above the running baseline.

    Implementation: shift the previous window's rate onto the current row,
    then alert when current > ALERT_FACTOR x previous (with a minimum volume
    so a single stray purchase in a dead window does not page anyone).
    """
    from pyspark.sql import Window

    baseline = Window.orderBy("window_start")

    return (
        metrics_df
        .withColumn("prev_conversion", F.lag("conversion_rate", 1).over(baseline))
        .withColumn(
            "is_anomaly",
            (F.col("views") >= ALERT_MIN_VIEWS)
            & F.col("prev_conversion").isNotNull()
            & (F.col("conversion_rate") > F.col("prev_conversion") * F.lit(float(ALERT_FACTOR))),
        )
        .filter(F.col("is_anomaly"))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Conversion spike: "),
                F.format_string("%.4f", F.col("conversion_rate")),
                F.lit(" vs baseline "),
                F.format_string("%.4f", F.col("prev_conversion")),
                F.lit(" in window "),
                F.col("window_start").cast(StringType()),
            ),
        )
    )


def start_metrics_sink(metrics_df):
    """Publish windowed summaries to the event-metrics topic."""
    out = metrics_df.select(
        F.to_json(
            F.struct(
                "window_start",
                "window_end",
                "event_count",
                "sessions",
                "views",
                "carts",
                "purchases",
                "revenue",
                "conversion_rate",
            )
        ).alias("value")
    )

    return (
        out.writeStream
        .format("kafka")  # required: without it Spark treats this as a file sink
        .outputMode("update")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_METRICS)
        .option("checkpointLocation", f"{CHECKPOINT}/metrics")
        .start()
    )


def start_alert_sink(alerts_df):
    """Publish anomaly alerts to the business-alerts topic."""
    out = alerts_df.select(
        F.col("window_start").cast(StringType()).alias("key"),
        F.to_json(
            F.struct("window_start", "window_end", "views", "purchases",
                     "conversion_rate", "prev_conversion", "alert_message")
        ).alias("value"),
    )

    return (
        out.writeStream
        .format("kafka")  # required: without it Spark treats this as a file sink
        .outputMode("update")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_ALERTS)
        .option("checkpointLocation", f"{CHECKPOINT}/alerts")
        .start()
    )


def main() -> None:
    spark = build_spark("ecommerce-streaming-etl")
    print("[streaming] starting...", flush=True)

    events = read_from_kafka(spark)
    metrics = aggregate_windows(events)

    # Both sinks run on the same streaming query's micro-batches.
    alerts = detect_anomalies(metrics)

    q_metrics = start_metrics_sink(metrics)
    q_alerts = start_alert_sink(alerts)

    print(f"[streaming] metrics query id = {q_metrics.id}", flush=True)
    print(f"[streaming] alerts  query id = {q_alerts.id}", flush=True)
    print(f"[streaming] running - watch Spark UI on :4040", flush=True)

    # Block forever: a stream has no "end".
    q_metrics.awaitTermination()


if __name__ == "__main__":
    main()
