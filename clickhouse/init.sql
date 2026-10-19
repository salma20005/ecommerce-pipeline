-- ============================================================
-- ClickHouse serving layer - table definitions
-- (Power BI reads these directly through the ClickHouse connector)
--
-- Apply with:
--   Get-Content init.sql -Raw | docker exec -i clickhouse \
--       clickhouse-client --multiquery
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce;

-- Funnel per day (from HDFS /curated/funnel_daily via Spark JDBC)
CREATE TABLE IF NOT EXISTS ecommerce.daily_funnel
(
    date             Date,
    viewed           UInt64,
    carted           UInt64,
    purchased        UInt64,
    view_to_cart     Float64,
    cart_to_purchase Float64,
    view_to_purchase Float64
)
-- ReplacingMergeTree: re-running the backfill replaces old rows on merge
ENGINE = ReplacingMergeTree
ORDER BY date;

-- Revenue per day (from HDFS /curated/revenue_daily via Spark JDBC)
CREATE TABLE IF NOT EXISTS ecommerce.daily_revenue
(
    date            Date,
    revenue         Float64,
    purchases       UInt64,
    buying_sessions UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY date;

-- Whole-month funnel, single row (from HDFS /curated/funnel)
CREATE TABLE IF NOT EXISTS ecommerce.funnel_overall
(
    viewed           UInt64,
    carted           UInt64,
    purchased        UInt64,
    view_to_cart     Float64,
    cart_to_purchase Float64,
    view_to_purchase Float64
)
ENGINE = ReplacingMergeTree
ORDER BY tuple();

-- Live 5-minute windows from Kafka topic "event-metrics"
-- (streaming sink uses output mode "update", so a window can arrive more
--  than once -> ReplacingMergeTree keeps only the newest version)
CREATE TABLE IF NOT EXISTS ecommerce.event_metrics
(
    window_start    DateTime,
    window_end      DateTime,
    event_count     UInt64,
    sessions        UInt64,
    views           UInt64,
    carts           UInt64,
    purchases       UInt64,
    revenue         Float64,
    conversion_rate Float64
)
ENGINE = ReplacingMergeTree
ORDER BY window_start;

-- Anomaly alerts from Kafka topic "business-alerts"
CREATE TABLE IF NOT EXISTS ecommerce.business_alerts
(
    window_start    DateTime,
    window_end      DateTime,
    views           UInt64,
    purchases       UInt64,
    conversion_rate Float64,
    prev_conversion Float64,
    alert_message   String
)
ENGINE = MergeTree
ORDER BY window_start;
