"""
FastAPI model-serving layer  (poster: "MODEL DEPLOYMENT (FASTAPI)").

Loads the winning Spark ML model from HDFS /models/best_model and exposes:

    GET  /         -> service info + the model that was actually loaded
    GET  /health   -> liveness probe
    POST /predict  -> probability that a browsing session ends in a purchase
    /docs          -> automatic interactive documentation (Swagger)

The feature order MUST match ml_training.py FEATURES:
    view_count, cart_count, unique_products, total_price, session_length_sec
event_count is deliberately absent: batch_etl guarantees
event_count = view_count + cart_count + purchase_count, so the model could
rebuild the label from it (that was the AUC = 1.0000 data leak).
purchase_count is absent because it IS the label.

Run inside a container built FROM the Spark image, because reading a Spark ML
model needs PySpark + the JVM + the HDFS client.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyspark.ml.classification import (
    GBTClassificationModel,
    LogisticRegressionModel,
    RandomForestClassificationModel,
)
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

MODEL_PATH = os.environ.get("MODEL_PATH", "hdfs://namenode:9000/models/best_model")

# Same order as the training FEATURES list in spark/ml_training.py
FEATURES = [
    "view_count",
    "cart_count",
    "unique_products",
    "total_price",
    "session_length_sec",
]

# Which model class to try, in order. best_model is whichever classifier won
# the AUC race in ml_training.py -- it is NOT always LogisticRegression, so we
# ask each loader until one accepts the saved metadata. (Each load reads a
# small metadata file first, so a mismatch fails fast and cheaply.)
LOADERS = {
    "logistic_regression": LogisticRegressionModel,
    "random_forest": RandomForestClassificationModel,
    "GBT": GBTClassificationModel,
}

spark: Optional[SparkSession] = None
model = None
model_name: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load Spark + the model ONCE at startup, not per request."""
    global spark, model, model_name
    spark = (
        SparkSession.builder
        .appName("purchase-prediction-api")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    # driver memory is a JVM flag; it must come from SPARK_DRIVER_MEMORY in the
    # Dockerfile -- setting it here would be silently ignored.
    spark.sparkContext.setLogLevel("ERROR")

    errors = {}
    for name, cls in LOADERS.items():
        try:
            model = cls.load(MODEL_PATH)
            model_name = name
            break
        except Exception as exc:  # noqa: BLE001 -- try the next loader
            errors[name] = str(exc).splitlines()[0][:120]

    if model is None:
        raise RuntimeError(
            f"could not load a model from {MODEL_PATH}. Tried: {errors}. "
            "Run spark/ml_training.py first -- it writes hdfs:///models/best_model."
        )
    print(f"[api] loaded {model_name} model from {MODEL_PATH}", flush=True)
    yield
    spark.stop()


app = FastAPI(
    title="Session Purchase Prediction API",
    description="Will this browsing session end in a purchase? "
                "Serves the best Spark MLlib model from HDFS.",
    version="1.0",
    lifespan=lifespan,
)


class SessionFeatures(BaseModel):
    view_count: int = Field(..., description="View events in the session", example=9)
    cart_count: int = Field(..., description="Add-to-cart events", example=2)
    unique_products: int = Field(..., description="Distinct products touched", example=5)
    total_price: float = Field(..., description="Sum of prices of all events", example=249.99)
    session_length_sec: int = Field(..., description="Session duration in seconds", example=640)


@app.get("/")
def root():
    return {
        "service": "session-purchase-prediction",
        "model": model_name,
        "model_path": MODEL_PATH,
        "features": FEATURES,
        "try_it": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None, "model": model_name}


@app.post("/predict")
def predict(f: SessionFeatures):
    if model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    vector = Vectors.dense([
        float(f.view_count),
        float(f.cart_count),
        float(f.unique_products),
        float(f.total_price),
        float(f.session_length_sec),
    ])
    df = spark.createDataFrame([(vector,)], ["features"])
    row = model.transform(df).select("probability", "prediction").collect()[0]
    probability = float(row["probability"][1])
    return {
        "model": model_name,
        "purchase_probability": round(probability, 4),
        "prediction": int(row["prediction"]),
        "will_purchase": bool(row["prediction"] == 1),
    }
