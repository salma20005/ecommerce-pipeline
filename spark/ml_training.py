"""
ml_training.py
==============
Stage 5 of the poster: "Session Purchase Prediction" (Spark MLlib).

Binary classification over hdfs:///curated/sessions -- one row per
browsing session; the question is: will this session end in a purchase?

Steps (printed as [ml] step k/7 while running):
  1. read the validated sessions parquet produced by batch_etl.py
  2. assemble the feature vector. NOTE: purchase_count is deliberately
     EXCLUDED -- purchased = (purchase_count > 0), so keeping it would be
     pure data leakage (the model would read the answer from the question).
  3. user-based 80/20 split: pmod(user_id, 5) == 0 -> test.
     The same user never appears in both sets (no leakage / no memorizing).
  4. class weights: only ~6.8% of sessions purchase (1:13 imbalance).
  5. train the 3 candidate models from the poster:
        LogisticRegression | RandomForest | GBT
     (weightCol is set only if the model supports it -- GBT does not)
  6. evaluate on the held-out USERS: AUC-ROC + binary P/R/F1 computed
     from one confusion-matrix pass (accuracy is useless here: predicting
     "never buys" already scores 93.2%).
  7. save every model to HDFS /models (winner also at /models/best_model)
     and write /data/ml_metrics.json -- one JSON row per model, ready to
     INSERT into the ClickHouse table ecommerce.model_metrics.

Run (no --packages needed, nothing to download):
    docker exec spark /opt/spark/bin/spark-submit \
        --master 'local[2]' \
        --driver-memory 2500m \
        --conf spark.sql.shuffle.partitions=16 \
        /opt/spark-jobs/ml_training.py
"""

import json
import time

from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SESSIONS = "hdfs://namenode:9000/curated/sessions"
MODELS_DIR = "hdfs:///models"
METRICS_JSON = "/data/ml_metrics.json"   # bind-mounted -> D:\\bigdata\\ml_metrics.json
LABEL = "purchased"
FEATURES = [
    "event_count",
    "view_count",
    "cart_count",
    "unique_products",
    "total_price",
    "session_length_sec",
    # purchase_count intentionally absent (leakage), user_id used only for the split
]

TRAIN_FRACTION_NOTE = "80% of users train / 20% of users test"


def log(msg: str) -> None:
    print(f"[ml] {msg}", flush=True)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("session-purchase-prediction")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def evaluate(preds_df):
    """One pass over the test set -> precision / recall / F1 of class 1."""
    agg = preds_df.agg(
        F.sum(((F.col("prediction") == 1) & (F.col(LABEL) == 1)).cast("long")).alias("tp"),
        F.sum(((F.col("prediction") == 1) & (F.col(LABEL) == 0)).cast("long")).alias("fp"),
        F.sum(((F.col("prediction") == 0) & (F.col(LABEL) == 1)).cast("long")).alias("fn"),
        F.count("*").alias("n"),
    ).collect()[0]

    tp, fp, fn, n = agg["tp"] or 0, agg["fp"] or 0, agg["fn"] or 0, agg["n"]
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return precision, recall, f1, n


def main() -> None:
    t_all = time.time()
    spark = build_spark()

    # ---- step 1/7: read -------------------------------------------------
    log(f"step 1/7: reading sessions from {SESSIONS}")
    sessions = spark.read.parquet(SESSIONS)
    log(f"       total sessions: {sessions.count():,}")

    # ---- step 2/7: features (no leakage) --------------------------------
    log(f"step 2/7: assembling features {FEATURES}")
    sessions = sessions.na.fill(0)
    sessions = (
        VectorAssembler(inputCols=FEATURES, outputCol="features")
        .transform(sessions)
        .select("user_id", LABEL, "features")
    )

    # ---- step 3/7: user-based split -------------------------------------
    log(f"step 3/7: split ({TRAIN_FRACTION_NOTE})")
    train = sessions.filter(F.pmod("user_id", F.lit(5)) != 0).cache()
    test = sessions.filter(F.pmod("user_id", F.lit(5)) == 0).cache()
    n_train = train.count()
    n_test = test.count()
    log(f"       train={n_train:,} sessions | test={n_test:,} sessions")

    # ---- step 4/7: class weights ----------------------------------------
    log("step 4/7: computing class weights (purchase is the minority)")
    counts = train.groupBy(LABEL).count().collect()
    n_pos = next((r["count"] for r in counts if r[LABEL] == 1), 1)
    n_neg = next((r["count"] for r in counts if r[LABEL] == 0), 1)
    n_total = n_pos + n_neg
    train = train.withColumn(
        "weight",
        F.when(F.col(LABEL) == 1, n_total / (2.0 * n_pos))
         .otherwise(n_total / (2.0 * n_neg)),
    ).cache()
    log(f"       positives={n_pos:,} negatives={n_neg:,} "
        f"-> weight(pos)={n_total / (2.0 * n_pos):.2f}")

    # ---- step 5/7: train the 3 candidates --------------------------------
    candidates = {
        "logistic_regression": LogisticRegression(
            featuresCol="features", labelCol=LABEL,
            weightCol="weight", maxIter=20,
        ),
        "random_forest": RandomForestClassifier(
            featuresCol="features", labelCol=LABEL,
            weightCol="weight", numTrees=30, maxDepth=8, seed=42,
        ),
        "GBT": GBTClassifier(
            featuresCol="features", labelCol=LABEL,
            maxIter=15, maxDepth=5, seed=42,   # GBT has no weightCol param
        ),
    }

    results = []
    fitted = {}
    for name, clf in candidates.items():
        if "weightCol" not in clf.getParamNames():
            log(f"step 5/7: {name} does not support weightCol -> training unweighted")
        t0 = time.time()
        log(f"step 5/7: training {name} ...")
        model = clf.fit(train)
        fitted[name] = model
        log(f"step 5/7: {name} trained in {time.time() - t0:.1f}s")

    # ---- step 6/7: evaluate on UNSEEN users ------------------------------
    log("step 6/7: evaluating on held-out users ...")
    auc_eval = BinaryClassificationEvaluator(
        labelCol=LABEL, rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    for name, model in fitted.items():
        t0 = time.time()
        preds = model.transform(test).cache()
        auc = auc_eval.evaluate(preds)
        precision, recall, f1, n = evaluate(preds)
        results.append({
            "model_name": name,
            "auc_roc": round(auc, 4),
            "f1": round(f1, 4),
            "precision_score": round(precision, 4),
            "recall_score": round(recall, 4),
            "test_sessions": int(n),
            "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        preds.unpersist()
        log(f"       {name}: AUC={auc:.4f} F1={f1:.4f} "
            f"P={precision:.4f} R={recall:.4f}  ({time.time() - t0:.1f}s)")

    # pretty table
    log("=" * 62)
    log(f"{'model':<22}{'AUC':>8}{'F1':>8}{'P':>8}{'R':>8}")
    for r in sorted(results, key=lambda x: -x["auc_roc"]):
        log(f"{r['model_name']:<22}{r['auc_roc']:>8.4f}{r['f1']:>8.4f}"
            f"{r['precision_score']:>8.4f}{r['recall_score']:>8.4f}")
    log("=" * 62)

    # ---- step 7/7: save models + metrics file ----------------------------
    best = max(results, key=lambda x: x["auc_roc"])
    for name, model in fitted.items():
        path = f"{MODELS_DIR}/{name}"
        model.write().overwrite().save(path)
        log(f"step 7/7: saved {name} -> {path}")
    fitted[best["model_name"]].write().overwrite().save(f"{MODELS_DIR}/best_model")
    log(f"step 7/7: BEST = {best['model_name']} -> {MODELS_DIR}/best_model")

    if "random_forest" in fitted:
        importances = fitted["random_forest"].featureImportances.toArray()
        ranked = sorted(zip(FEATURES, importances), key=lambda x: -x[1])
        log("       RF feature importance: " +
            ", ".join(f"{f}={v:.2f}" for f, v in ranked))

    with open(METRICS_JSON, "w", encoding="utf-8") as fh:
        for r in results:
            fh.write(json.dumps(r) + "\n")
    log(f"step 7/7: metrics rows written to {METRICS_JSON}")
    log(f"done in {time.time() - t_all:.0f}s total")

    train.unpersist()
    test.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
