"""
Day 5 — Windowed aggregations persisted to PostgreSQL.

Architecture
────────────
Both streaming queries from Day 4 now sink into the ecom schema via
foreachBatch + psycopg2 upserts, replacing the console-only output:

  ecom.hourly_revenue    ← 1-minute tumbling window  (revenue per window)
  ecom.category_metrics  ← 5-minute sliding window   (view/purchase per category)

Why foreachBatch instead of the native JDBC sink
─────────────────────────────────────────────────
  1. The JDBC streaming sink only supports append mode and issues plain
     INSERT statements — it cannot run ON CONFLICT DO UPDATE upserts.
  2. foreachBatch exposes each micro-batch as a bounded DataFrame, letting
     us call any Spark or Python I/O we need.
  3. psycopg2's execute_values() sends an entire batch in one round-trip,
     which is far more efficient than one INSERT per row.

Idempotency guarantee
─────────────────────
  INSERT … ON CONFLICT (pk) DO UPDATE SET … (UPSERT) means that if Spark
  replays a micro-batch on restart (at-least-once delivery), the database
  converges to the correct value rather than producing duplicates.

Locking behaviour
─────────────────
  ON CONFLICT DO UPDATE takes row-level locks only — no table-level lock.
  The two queries write to different tables, so they never contend.

foreachBatch runs on the Spark driver (stream-processor container), not on
executors, so psycopg2 only needs to be installed there.
"""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    round as _round,
    sum as _sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "kafka:29092")
TOPIC           = os.getenv("KAFKA_TOPIC_CLICKSTREAM",   "ecom.clickstream")
TRIGGER_SECS    = os.getenv("TRIGGER_INTERVAL_SECS",     "10")
WATERMARK       = os.getenv("WATERMARK_DURATION",         "10 minutes")
REVENUE_WIN     = os.getenv("REVENUE_WINDOW_DURATION",    "1 minute")
ACTIVITY_WIN    = os.getenv("ACTIVITY_WINDOW_DURATION",   "5 minutes")
ACTIVITY_SLIDE  = os.getenv("ACTIVITY_SLIDE_DURATION",    "1 minute")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR",             "/tmp/spark-checkpoints")

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
PG_HOST = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB",       "ecom_db")
PG_USER = os.getenv("POSTGRES_USER",     "ecom_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "ecom_pass")

# ── Upsert SQL ─────────────────────────────────────────────────────────────────
# EXCLUDED.col refers to the value that was rejected by the conflict check.
# ON CONFLICT DO UPDATE acquires a row-level lock only — no table lock.

REVENUE_UPSERT_SQL = """
    INSERT INTO ecom.hourly_revenue
        (window_start, window_end, total_revenue, purchase_count)
    VALUES %s
    ON CONFLICT (window_start, window_end) DO UPDATE SET
        total_revenue  = EXCLUDED.total_revenue,
        purchase_count = EXCLUDED.purchase_count,
        updated_at     = NOW()
"""

METRICS_UPSERT_SQL = """
    INSERT INTO ecom.category_metrics
        (window_start, window_end, category, action, event_count)
    VALUES %s
    ON CONFLICT (window_start, window_end, category, action) DO UPDATE SET
        event_count = EXCLUDED.event_count,
        updated_at  = NOW()
"""

# ── Schema (mirrors generator.py) ─────────────────────────────────────────────
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",   StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category",   StringType(), True),
    StructField("action",     StringType(), True),
    StructField("price",      FloatType(),  True),
    StructField("page_url",   StringType(), True),
])


# ── PostgreSQL helpers ─────────────────────────────────────────────────────────

def pg_connect() -> psycopg2.extensions.connection:
    """
    Open a fresh psycopg2 connection pinned to UTC.
    A new connection per foreachBatch call is intentional — foreachBatch
    is sequential within a query, so we never hold connections between batches.
    """
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        options="-c timezone=UTC",   # ensure timestamps land in UTC
    )


def pg_upsert(rows: list[dict], sql: str, col_order: tuple[str, ...]) -> None:
    """
    Bulk-upsert a list of row-dicts into Postgres.

    execute_values() sends all rows as a single multi-row INSERT in one
    network round-trip — far cheaper than executemany() for large batches.
    page_size controls how many rows are bundled per VALUES() clause.
    """
    tuples = [tuple(row[c] for c in col_order) for row in rows]
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=200)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Spark session ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecom-windowed-aggregations")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Base stream (shared by both queries) ──────────────────────────────────────

def read_base_stream(spark: SparkSession) -> DataFrame:
    """
    Read raw Kafka messages → parse JSON → cast timestamp → apply watermark.

    Both streaming queries consume this single logical DataFrame.  Spark
    creates two independent Kafka readers under the hood (one per query),
    each with its own consumer group and offset tracking.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .withColumn("event", from_json(col("json_value"), CLICKSTREAM_SCHEMA))
        .select(
            col("event.user_id"),
            col("event.category"),
            col("event.action"),
            col("event.price"),
            # to_timestamp handles ISO-8601 strings with timezone offsets (+00:00)
            to_timestamp(col("event.timestamp")).alias("event_time"),
        )
        # Watermark: Spark drops events that arrive more than WATERMARK after
        # the maximum observed event_time and frees their window state.
        .withWatermark("event_time", WATERMARK)
    )


# ── foreachBatch writers ───────────────────────────────────────────────────────

def write_revenue_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler for the revenue query.

    Runs on the Spark driver once per micro-batch trigger.  The batch_df
    is a bounded (non-streaming) DataFrame containing only the windows that
    the watermark has finalised since the last trigger.

    Flow:
      persist → collect to driver → log summary → upsert → unpersist
    """
    # persist() so Spark doesn't recompute the batch for collect() and show()
    batch_df.persist()
    try:
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows:
            return

        # ── Console log ──────────────────────────────────────────────────────
        print(f"\n{'─'*62}")
        print(f"  [revenue_per_minute]  batch={batch_id}  windows finalised={len(rows)}")
        print(f"{'─'*62}")
        for r in rows:
            print(
                f"  {r['window_start']}  →  {r['window_end']}"
                f"   revenue=${r['total_revenue']:<10.2f}  purchases={r['purchase_count']}"
            )

        # ── Postgres upsert ───────────────────────────────────────────────────
        pg_upsert(
            rows,
            REVENUE_UPSERT_SQL,
            ("window_start", "window_end", "total_revenue", "purchase_count"),
        )
        print(f"  ✓ {len(rows)} row(s) upserted → ecom.hourly_revenue")
    finally:
        batch_df.unpersist()


def write_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler for the action-count query.

    Same pattern as write_revenue_batch.  Logs a condensed summary (up to
    10 lines) to avoid flooding the console — the full result is in Postgres.
    """
    batch_df.persist()
    try:
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows:
            return

        # ── Console log (condensed) ───────────────────────────────────────────
        print(f"\n{'─'*62}")
        print(f"  [action_counts_5min]  batch={batch_id}  rows finalised={len(rows)}")
        print(f"{'─'*62}")
        for r in rows[:10]:                             # show first 10 to avoid spam
            print(
                f"  {r['window_start']}  →  {r['window_end']}"
                f"   {r['category']:<16} {r['action']:<12} count={r['event_count']}"
            )
        if len(rows) > 10:
            print(f"  … and {len(rows) - 10} more rows (see ecom.category_metrics)")

        # ── Postgres upsert ───────────────────────────────────────────────────
        pg_upsert(
            rows,
            METRICS_UPSERT_SQL,
            ("window_start", "window_end", "category", "action", "event_count"),
        )
        print(f"  ✓ {len(rows)} row(s) upserted → ecom.category_metrics")
    finally:
        batch_df.unpersist()


# ── Windowed aggregations ──────────────────────────────────────────────────────

def start_revenue_query(base_df: DataFrame):
    """
    Metric 1 — Total purchase revenue per 1-minute tumbling window.

    Tumbling windows are non-overlapping; each purchase event falls into
    exactly one window.  Output mode 'append' means Spark emits a row only
    after the watermark has advanced past window_end — guaranteeing the
    window will not receive any more late data.
    """
    revenue_df = (
        base_df
        .filter(col("action") == "purchase")
        .groupBy(window(col("event_time"), REVENUE_WIN))
        .agg(
            _round(_sum("price"), 2).alias("total_revenue"),
            count("*").alias("purchase_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_revenue",
            "purchase_count",
        )
    )

    return (
        revenue_df.writeStream
        .queryName("revenue_per_minute")
        .outputMode("append")
        .foreachBatch(write_revenue_batch)
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/revenue_window")
        .start()
    )


def start_action_counts_query(base_df: DataFrame):
    """
    Metric 2 — View and purchase counts per category in 5-minute sliding windows.

    Sliding windows overlap: each event with windowDuration=5m / slideDuration=1m
    falls into 5 consecutive windows.  This gives a rolling 5-minute picture
    that updates every minute once windows begin to finalise.
    """
    activity_df = (
        base_df
        .filter(col("action").isin("view", "purchase"))
        .groupBy(
            window(col("event_time"), ACTIVITY_WIN, ACTIVITY_SLIDE),
            col("category"),
            col("action"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "category",
            "action",
            "event_count",
        )
    )

    return (
        activity_df.writeStream
        .queryName("action_counts_5min")
        .outputMode("append")
        .foreachBatch(write_metrics_batch)
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/action_window")
        .start()
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 62)
    print("  ecom-windowed-aggregations — Day 5 (PostgreSQL sink)")
    print("=" * 62)
    print(f"  Kafka     : {KAFKA_BOOTSTRAP}  →  {TOPIC}")
    print(f"  Postgres  : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"  Watermark : {WATERMARK}  (set WATERMARK_DURATION=1 minute for dev)")
    print(f"  Revenue   : tumbling {REVENUE_WIN}")
    print(f"  Activity  : sliding {ACTIVITY_WIN} / slide {ACTIVITY_SLIDE}")
    print(f"  Trigger   : every {TRIGGER_SECS}s")
    print("=" * 62 + "\n")

    base_df = read_base_stream(spark)

    q1 = start_revenue_query(base_df)
    q2 = start_action_counts_query(base_df)

    print(f"[INFO] revenue query  : {q1.name}  id={q1.id}")
    print(f"[INFO] activity query : {q2.name}  id={q2.id}")
    print(
        "\n[NOTE] With append output mode, rows are written to Postgres only after\n"
        f"       the watermark passes window_end.  First writes appear roughly\n"
        f"       {WATERMARK} after the producer starts sending purchase events.\n"
    )

    # Block until any query errors or the process receives SIGTERM / Ctrl-C.
    # The finally block stops the surviving query so it can checkpoint before exit.
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for q in spark.streams.active:
            q.stop()


if __name__ == "__main__":
    main()
