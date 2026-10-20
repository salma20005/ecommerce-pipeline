"""
FastAPI model-serving layer  (poster: "MODEL DEPLOYMENT (FASTAPI)").

Loads the winning Spark ML model from HDFS /models/best_model and exposes:

    GET  /         -> service info
    POST /predict  -> probability that a browsing session ends in a purchase
    /docs          -> automatic interactive documentation (Swagger)

The feature order MUST match ml_training.py FEATURES. purchase_count is
deliberately absent (it is the label -> data leakage).

Run inside a container built FROM the Spark image, because reading a Spark
ML model needs PySpark + the JVM + the HDFS client.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

MODEL_PATH = os.environ.get("MODEL_PATH", "hdfs://namenode:9000/models/best_model")

# Same order as the training FEATURES list in spark/ml_training.py
FEATURES = [
    "event_count",
    "view_count",
    "cart_count",
    "unique_products",
    "total_price",
    "session_length_sec",
]

spark: Optional[SparkSession] = None
model: Optional[LogisticRegressionModel] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load Spark + the model ONCE at startup, not per request."""
    global spark, model
    spark = (
        SparkSession.builder
        .appName("purchase-prediction-api")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    model = LogisticRegressionModel.load(MODEL_PATH)
    print(f"[api] loaded model from {MODEL_PATH}", flush=True)
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
    event_count: int = Field(..., description="Total events in the session", example=12)
    view_count: int = Field(..., description="View events", example=9)
    cart_count: int = Field(..., description="Add-to-cart events", example=2)
    unique_products: int = Field(..., description="Distinct products touched", example=5)
    total_price: float = Field(..., description="Sum of prices of all events", example=249.99)
    session_length_sec: int = Field(..., description="Session duration in seconds", example=640)


@app.get("/")
def root():
    return {
        "service": "session-purchase-prediction",
        "model": "best_model (LogisticRegression from HDFS /models/best_model)",
        "features": FEATURES,
        "try_it": "/docs",
    }


@app.post("/predict")
def predict(f: SessionFeatures):
    vector = Vectors.dense([
        float(f.event_count),
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
        "purchase_probability": round(probability, 4),
        "prediction": int(row["prediction"]),
        "will_purchase": bool(row["prediction"] == 1),
    }
