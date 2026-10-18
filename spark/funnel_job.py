"""
funnel_job.py
=============
Completes Stage 3A of the poster: "Funnel & revenue metrics
(view -> cart -> purchase)".

The batch ETL produces session-level data. This job adds the FUTURE
view - a funnel table - which answers:
    Of 100 people who VIEWED a product, how many added it to the cart,
    and how many actually BOUGHT it?

That funnel is what the "Business Overview" dashboard in Power BI shows.

It reads the cleaned events already written by batch_etl.py to
/processed/events_clean and writes two tables:

    /curated/funnel           - overall funnel (one row, whole month)
    /curated/funnel_daily     - funnel per day (trend chart)

Run AFTER batch_etl.py has finished:

    docker exec spark /opt/spark/bin/spark-submit \
        --master 'local[2]' \
        --driver-memory 3g \
        /opt/spark-jobs/funnel_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_PROCESSED = "hdfs://namenode:9000/processed/events_clean"
HDFS_FUNNEL = "hdfs://namenode:9000/curated/funnel"
HDFS_FUNNEL_DAILY = "hdfs://namenode:9000/curated/funnel_daily"
HDFS_REVENUE = "hdfs://namenode:9000/curated/revenue_daily"


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def funnel_counts(df):
    """Three numbers that define the funnel: viewed, carted, purchased.

    Note we count EVENTS, not sessions - the funnel is about product
    interest at each step, so one session may contribute several views.
    """
    return df.agg(
        F.sum((F.col("event_type") == "view").cast("long")).alias("viewed"),
        F.sum((F.col("event_type") == "cart").cast("long")).alias("carted"),
        F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchased"),
    )


def add_rates(row):
    """Convert raw counts into the percentages a dashboard actually shows."""
    viewed = row["viewed"] or 0
    carted = row["carted"] or 0
    purchased = row["purchased"] or 0

    # Each rate is measured against the PREVIOUS step, which is how
    # funnel drop-off is conventionally reported.
    return (
        viewed,
        carted,
        purchased,
        (carted / viewed) if viewed else 0.0,          # view -> cart
        (purchased / carted) if carted else 0.0,        # cart -> purchase
        (purchased / viewed) if viewed else 0.0,        # view -> purchase (overall)
    )


def main() -> None:
    spark = build_spark("ecommerce-funnel")

    events = spark.read.parquet(HDFS_PROCESSED)
    print(f"[funnel] reading from {HDFS_PROCESSED}", flush=True)

    # ---- 1. overall funnel (whole month, one row) -------------------------
    overall = funnel_counts(events).collect()[0]
    o_viewed, o_carted, o_purchased, o_v2c, o_c2p, o_v2p = add_rates(overall)

    overall_df = spark.createDataFrame([(
        int(o_viewed), int(o_carted), int(o_purchased),
        float(o_v2c), float(o_c2p), float(o_v2p),
    )], "viewed long, carted long, purchased long, "
        "view_to_cart double, cart_to_purchase double, view_to_purchase double")

    (overall_df.write.mode("overwrite").parquet(HDFS_FUNNEL))
    print(f"[funnel] overall: viewed={o_viewed:,} carted={o_carted:,} "
          f"purchased={o_purchased:,}", flush=True)
    print(f"[funnel] view->cart = {o_v2c:.4%} | cart->purchase = {o_c2p:.4%} "
          f"| overall = {o_v2p:.4%}", flush=True)

    # ---- 2. daily funnel (for the trend chart) ----------------------------
    daily_counts = (
        events
        .groupBy("date")
        .agg(
            F.sum((F.col("event_type") == "view").cast("long")).alias("viewed"),
            F.sum((F.col("event_type") == "cart").cast("long")).alias("carted"),
            F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchased"),
        )
    )

    # Spark SQL UDF-free way to compute the ratios per row:
    daily = (
        daily_counts
        .withColumn("view_to_cart",
                    F.when(F.col("viewed") > 0, F.col("carted") / F.col("viewed")).otherwise(0.0))
        .withColumn("cart_to_purchase",
                    F.when(F.col("carted") > 0, F.col("purchased") / F.col("carted")).otherwise(0.0))
        .withColumn("view_to_purchase",
                    F.when(F.col("viewed") > 0, F.col("purchased") / F.col("viewed")).otherwise(0.0))
        .orderBy("date")
    )

    (daily.write.mode("overwrite").parquet(HDFS_FUNNEL_DAILY))
    print(f"[funnel] wrote {HDFS_FUNNEL_DAILY}", flush=True)

    # ---- 3. daily revenue (the poster asks for "revenue metrics") ---------
    revenue = (
        events
        .filter(F.col("event_type") == "purchase")
        .groupBy("date")
        .agg(
            F.sum("price").alias("revenue"),
            F.count("*").alias("purchases"),
            F.count_distinct("user_session").alias("buying_sessions"),
        )
        .orderBy("date")
    )

    (revenue.write.mode("overwrite").parquet(HDFS_REVENUE))
    print(f"[funnel] wrote {HDFS_REVENUE}", flush=True)

    spark.stop()
    print("[funnel] done.", flush=True)


if __name__ == "__main__":
    main()
