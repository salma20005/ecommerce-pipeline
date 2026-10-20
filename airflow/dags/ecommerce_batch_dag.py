"""
ecommerce_batch_dag.py
======================
Stage 6 of the poster: "AIRFLOW - BATCH WORKFLOW ORCHESTRATION".

One manual run = one full pass of the batch pipeline:

    validate_input_file           Airflow: file exists, ~5 GB, right header
      -> batch_etl                Spark: clean 42M events -> HDFS parquet
      -> funnel_job               Spark: funnel + revenue daily tables
      -> validate_outputs         HDFS: _SUCCESS markers must be there
      -> publish_to_clickhouse    Spark JDBC -> ClickHouse  ("publish metrics")
      -> validate_serving_layer   ClickHouse: 31 funnel days / 1 overall row

Design notes (you will be asked about these):
  * Every Spark task is the EXACT docker-exec command that was proven by
    hand in the README -- Airflow owns ordering / retries / logs / validation
    and moves NO data itself.
  * schedule=None on purpose: the source is ONE static historical file
    (2019-Oct.csv), so a run is started by hand from the UI. A cron would
    just re-clean the same 5 GB forever.
  * max_active_runs=1: two 5-GB Spark runs must never pile up on a laptop.
  * The two validate_* tasks are the poster's "Failure Handling + Data
    Validation": fail fast BEFORE Spark burns 40 minutes, and fail loudly
    BEFORE anyone puts a wrong number in Power BI.

UI: http://localhost:8080  (admin / admin) -> DAG "ecommerce_batch_pipeline"
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Mounted read-only into the scheduler: D:/bigdata:/data:ro
CSV_PATH = Path("/data/2019-Oct.csv")
EXPECTED_HEADER = (
    "event_time,event_type,product_id,category_id,category_code,"
    "brand,price,user_id,user_session"
)

default_args = {
    "owner": "ecommerce-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def check_input_file() -> None:
    """Fail fast before Spark burns 40 minutes on a missing/broken CSV."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"{CSV_PATH} not found - download REES46 '2019-Oct.csv' from "
            "Kaggle and place it in D:/bigdata"
        )
    size_gb = CSV_PATH.stat().st_size / 1e9
    if size_gb < 4.0:
        raise ValueError(f"{CSV_PATH} is only {size_gb:.2f} GB, expected ~5.3 GB")
    with CSV_PATH.open(encoding="utf-8") as fh:
        header = fh.readline().strip()
    if header != EXPECTED_HEADER:
        raise ValueError(f"unexpected CSV header: {header!r}")
    print(f"input OK: {CSV_PATH} ({size_gb:.2f} GB), header matches schema")


with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="validate -> Spark batch -> validate -> publish to ClickHouse",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,            # static historical file -> runs start by hand
    catchup=False,
    max_active_runs=1,        # never pile two 5-GB runs on the laptop
    dagrun_timeout=timedelta(hours=3),
    tags=["ecommerce", "spark", "poster"],
    doc_md=__doc__,
) as dag:

    validate_input_file = PythonOperator(
        task_id="validate_input_file",
        python_callable=check_input_file,
        doc_md="Exists + >= 4 GB + exact 9-column header. No Spark touched yet.",
    )

    batch_etl = BashOperator(
        task_id="batch_etl",
        bash_command="""
set -euo pipefail
docker exec spark /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    --driver-memory 2500m \
    --conf spark.sql.shuffle.partitions=16 \
    /opt/spark-jobs/batch_etl.py
""",
        doc_md="Clean + enrich + aggregate to sessions -> HDFS parquet.",
    )

    funnel_job = BashOperator(
        task_id="funnel_job",
        bash_command="""
set -euo pipefail
docker exec spark /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    --driver-memory 2500m \
    --conf spark.sql.shuffle.partitions=16 \
    /opt/spark-jobs/funnel_job.py
""",
        doc_md="view -> cart -> purchase funnel + daily revenue.",
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command="""
set -euo pipefail
docker exec namenode hdfs dfs -test -e /curated/sessions/_SUCCESS
docker exec namenode hdfs dfs -test -e /curated/funnel_daily/_SUCCESS
docker exec namenode hdfs dfs -test -e /curated/revenue_daily/_SUCCESS
echo "HDFS outputs OK: sessions + funnel_daily + revenue_daily all committed"
""",
        doc_md="_SUCCESS markers = Spark committed every output. "
               "Gate before anything is published.",
    )

    publish_to_clickhouse = BashOperator(
        task_id="publish_to_clickhouse",
        bash_command="""
set -euo pipefail
docker exec spark /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    --driver-memory 1g \
    --jars /opt/spark-jdbc/clickhouse-jdbc-all.jar \
    /opt/spark-jobs/ch_backfill.py
""",
        doc_md="Spark JDBC writer: HDFS parquet -> ClickHouse serving layer.",
    )

    validate_serving_layer = BashOperator(
        task_id="validate_serving_layer",
        bash_command="""
set -euo pipefail
DAYS=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM ecommerce.daily_funnel")
[ "$DAYS" -ge 31 ] || { echo "daily_funnel has $DAYS rows, expected 31"; exit 1; }
OVERALL=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM ecommerce.funnel_overall")
[ "$OVERALL" -ge 1 ] || { echo "funnel_overall is empty"; exit 1; }
echo "ClickHouse serving layer OK: daily_funnel=$DAYS days, funnel_overall=$OVERALL row(s)"
""",
        doc_md="Numbers Power BI will read must actually be there.",
    )

    (
        validate_input_file
        >> batch_etl
        >> funnel_job
        >> validate_outputs
        >> publish_to_clickhouse
        >> validate_serving_layer
    )
