<#
start_streaming.ps1
===================
Brings up the REAL-TIME half of the pipeline in the right order:

    kafka  ->  producer  ->  spark streaming_etl  ->  ch-loader  ->  ClickHouse

Order matters: the producer exits with NoBrokersAvailable if Kafka is not
READY yet, and the streaming job silently drops everything if the old
checkpoint has a newer watermark than the replayed data.

The three gotchas on this laptop / image (all hit in practice):
  1. Spark's image user has HOME=/nonexistent, so Ivy cannot write its
     cache -> pass -Divy.home=/tmp/.ivy2 through SPARK_SUBMIT_OPTS
     (the Kafka connector jars are already cached in /tmp/.ivy2).
  2. --driver-memory 1024m, NOT the 3g from the docstring:
     5.2 GB WSL + swap on an HDD cannot afford 3g while Airflow,
     ClickHouse, HDFS and Kafka run at the same time.
  3. If business-alerts / event-metrics look "stuck", the fix is a fresh
     checkpoint (the replay restarts from 2019-10-01 while an old
     checkpoint watermark sits at 2019-10-06 -> everything is late data).
     See "Recovery" at the bottom of this file.

Safe to re-run: it refuses to start a second streaming job.

Usage:
    powershell -ExecutionPolicy Bypass -File scripts\start_streaming.ps1
#>

$ErrorActionPreference = "Continue"

function Test-ContainerRunning($name) {
    $out = docker ps --filter "name=^$name$" --filter "status=running" --format "{{.Names}}"
    return ($out -contains $name)
}

Write-Host "[1/6] containers"

# --- spark (needed for the streaming job itself) --------------------------
if (-not (Test-ContainerRunning "spark")) { docker start spark | Out-Null }

# --- 1. Kafka first: everything else depends on the broker ----------------
if (-not (Test-ContainerRunning "kafka")) {
    Write-Host "       kafka is down -> starting it"
    docker start kafka | Out-Null
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {          # up to 150 s (KRaft is slow)
        Start-Sleep -Seconds 5
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 --list 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Write-Host "       waiting for the broker ..."
    }
    if (-not $ready) { Write-Host "ERROR: kafka never became ready"; exit 1 }
}
Write-Host "       kafka OK"

# --- 2. producer (replays the CSV into user-events) -----------------------
if (-not (Test-ContainerRunning "producer")) {
    # start AFTER the broker is ready, otherwise NoBrokersAvailable
    docker start producer | Out-Null
    Start-Sleep -Seconds 10
}
if (-not (Test-ContainerRunning "producer")) {
    Write-Host "ERROR: producer did not stay up - check: docker logs producer"
    exit 1
}
Write-Host "       producer OK"

# --- 3. ch-loader (the Kafka -> ClickHouse bridge, cannot be removed) -----
if (-not (Test-ContainerRunning "ch-loader")) { docker start ch-loader | Out-Null }
Write-Host "       ch-loader OK"

# --- 4. streaming job (refuse to run it twice) ----------------------------
Write-Host "[2/6] streaming job"
$alive = docker exec spark sh -c `
    "ps aux | grep streaming_etl | grep -v grep | wc -l" 2>$null
if ($alive -and ([int]($alive.Trim()) -gt 0)) {
    Write-Host "       already running - nothing to do"
} else {
    Write-Host "[3/6] spark-submit streaming_etl.py (driver 1024m, ivy.home fix)"
    docker exec -d -e SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2" spark sh -c `
        "/opt/spark/bin/spark-submit --master local[2] --driver-memory 1024m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/spark-jobs/streaming_etl.py > /tmp/streaming.log 2>&1"

    # --- 5. wait until both streaming queries are up ----------------------
    Write-Host "[4/6] waiting for the queries (cold start is ~2 min)"
    $started = $false
    for ($i = 0; $i -lt 40; $i++) {         # up to 200 s
        Start-Sleep -Seconds 5
        docker exec spark grep -F "running - watch Spark UI" /tmp/streaming.log 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $started = $true; break }
        if ($i % 6 -eq 5) { Write-Host "       still starting ..." }
    }
    if (-not $started) {
        Write-Host "WARN: did not see the ready marker yet - inspect:"
        Write-Host "      docker exec spark tail -30 /tmp/streaming.log"
    } else {
        Write-Host "       streaming queries are UP"
    }
}

# --- 6. report -------------------------------------------------------------
Write-Host "[5/6] current serving-layer rows"
docker exec clickhouse clickhouse-client --query `
    "SELECT 'event_metrics', count() FROM ecommerce.event_metrics UNION ALL SELECT 'business_alerts', count() FROM ecommerce.business_alerts FORMAT TSV" 2>$null

Write-Host "[6/6] done"
Write-Host "  - Power BI connects to  localhost:18123  (not 8123)"
Write-Host "  - event_metrics keeps growing while the producer replays"
Write-Host "  - business_alerts stays 0 unless conversion > 3x baseline"
Write-Host ""
Write-Host "Recovery (stuck counts): docker exec spark pkill -9 -f SparkSubmit"
Write-Host "  docker exec spark mv /data/checkpoints/streaming /data/checkpoints/streaming_OLD"
Write-Host "  then re-run this script (fresh checkpoint = fresh watermark)."
