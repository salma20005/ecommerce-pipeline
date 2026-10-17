# E-Commerce User Behavior Analytics & Purchase Prediction

An End-to-End Big Data pipeline using **Apache Kafka, HDFS, Apache Spark, ClickHouse, Power BI, FastAPI, and Docker Compose**.

## Pipeline

```
CSV (historical replay) → Kafka → Spark Batch / Structured Streaming → HDFS / ClickHouse → Power BI
```

| Component | Role |
|---|---|
| Kafka | Event streaming backbone (topics: `user-events`, `event-metrics`, `business-alerts`) |
| Producer | Replays `2019-Oct.csv` into Kafka as if it were live clickstream data (keyed by `user_session`) |
| HDFS | Distributed file storage (`/raw`, `/processed`, `/curated`, `/models`) |
| Spark | Batch ETL + Structured Streaming (5-min windows, anomaly detection) |
| MLlib | Session purchase prediction (Logistic Regression / Random Forest / GBT) |
| ClickHouse | Analytical serving layer for dashboards |
| FastAPI | Model serving (`POST /predict`) |
| Airflow | Batch workflow orchestration |
| Power BI | Dashboards (connects to ClickHouse via DirectQuery) |

## Quick start

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it.
2. Download `2019-Oct.csv` from Kaggle (RE46 eCommerce Behavior Data) and place it in `D:/bigdata/2019-Oct.csv`.
3. Run:
   ```powershell
   cd ecommerce-pipeline
   docker compose up -d
   ```
4. Monitor:
   - Kafka UI: http://localhost:8081
   - HDFS UI: http://localhost:9870
   - Spark UI: http://localhost:4040 (while a job runs)

## Team

- Member 1 — Kafka + producer
- Member 2 — Spark batch/streaming + HDFS + MLlib
- Member 3 — ClickHouse + FastAPI + Power BI
