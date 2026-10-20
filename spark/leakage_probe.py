"""Leakage probe: is total_price>0 perfectly correlated with the label?

Run:
  docker exec spark /opt/spark/bin/spark-submit --master 'local[2]' \
      --driver-memory 1500m /opt/spark-jobs/leakage_probe.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("leakage-probe")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/curated/sessions")

print("\n=== 1) does total_price > 0 reveal the label? ===", flush=True)
df.groupBy("purchased").agg(
    F.count("*").alias("sessions"),
    F.sum((F.col("total_price") > 0).cast("long")).alias("with_price_gt_0"),
    F.sum((F.col("total_price") == 0).cast("long")).alias("with_price_eq_0"),
    F.avg("total_price").alias("avg_price"),
    F.avg("cart_count").alias("avg_cart"),
    F.avg("event_count").alias("avg_events"),
).show(truncate=False)

print("=== 2) is purchase_count == label? (pure leakage check) ===", flush=True)
df.groupBy("purchased", (F.col("purchase_count") > 0).cast("int").alias("pc_gt_0")).count().show()

print("=== 3) event_count == view+cart+purchase? (hidden leakage) ===", flush=True)
df.select(
    (F.col("event_count") == F.col("view_count") + F.col("cart_count") + F.col("purchase_count"))
    .alias("sums_exactly")
).groupBy("sums_exactly").count().show()

spark.stop()
