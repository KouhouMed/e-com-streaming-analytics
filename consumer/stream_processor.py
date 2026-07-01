"""
Day 4 — Real-time windowed aggregations with late-data handling.

Two streaming queries run concurrently against the same Kafka topic:

  Query 1 — revenue_per_minute
    Tumbling 1-minute windows over purchase events.
    Computes total revenue and purchase count per window.

  Query 2 — action_counts_5min
    Sliding 5-minute windows (1-minute slide) over view and purchase events.
    Computes per-category action breakdown.

Watermark contract
------------------
Both queries share a 10-minute event-time watermark.  This means:

  • Spark accepts late events that arrive up to WATERMARK after the maximum
    observed event_time.  Later arrivals are silently dropped.
  • Window state is finalised and discarded once the watermark advances past
    window_end.
  • With "append" output mode, a row is emitted exactly once — when its
    window is guaranteed to be complete (i.e. after watermark passes
    window_end).

Development tip
---------------
With a 10-minute watermark, the first rows appear roughly 11 minutes after
the producer starts (1-min window + 10-min watermark).  For faster feedback
during development set  WATERMARK_DURATION=1 minute  in .env.
"""

import os

from pyspark.sql import SparkSession
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

# ── Configuration (all overridable via .env / docker-compose environment) ────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "kafka:29092")
TOPIC           = os.getenv("KAFKA_TOPIC_CLICKSTREAM",   "ecom.clickstream")
TRIGGER_SECS    = os.getenv("TRIGGER_INTERVAL_SECS",     "10")
WATERMARK       = os.getenv("WATERMARK_DURATION",         "10 minutes")
REVENUE_WIN     = os.getenv("REVENUE_WINDOW_DURATION",    "1 minute")
ACTIVITY_WIN    = os.getenv("ACTIVITY_WINDOW_DURATION",   "5 minutes")
ACTIVITY_SLIDE  = os.getenv("ACTIVITY_SLIDE_DURATION",    "1 minute")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR",             "/tmp/spark-checkpoints")

# ── Schema — must stay in sync with producer/generator.py ─────────────────────
# timestamp is kept as StringType here; we cast it to TimestampType below so
# the window() and withWatermark() functions can operate on it correctly.
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


# ── Spark session ──────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecom-windowed-aggregations")
        # Low shuffle partitions — we have a single Kafka partition in dev
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Base stream ────────────────────────────────────────────────────────────────
def read_base_stream(spark: SparkSession):
    """
    Read raw Kafka messages, deserialise JSON, and apply the event-time
    watermark.  Returns a single streaming DataFrame that both aggregation
    queries consume independently.

    The ISO-8601 timestamp produced by generator.py
    (e.g. "2026-07-01T10:00:00.123456+00:00") is converted to a native
    TimestampType column named event_time using to_timestamp(), which handles
    timezone offsets automatically in Spark 3.x.
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

    parsed_df = (
        raw_df
        # The Kafka value column is binary — decode to UTF-8 first
        .selectExpr("CAST(value AS STRING) AS json_value")
        .withColumn("event", from_json(col("json_value"), CLICKSTREAM_SCHEMA))
        .select(
            col("event.event_id"),
            col("event.user_id"),
            col("event.product_id"),
            col("event.category"),
            col("event.action"),
            col("event.price"),
            # Cast the ISO-8601 string to a proper timestamp for windowing
            to_timestamp(col("event.timestamp")).alias("event_time"),
        )
        # ── Watermark ─────────────────────────────────────────────────────────
        # Instructs Spark to:
        #   1. Drop events whose event_time < (max_seen_event_time - WATERMARK)
        #   2. Finalise window state once the watermark passes window_end
        .withWatermark("event_time", WATERMARK)
    )
    return parsed_df


# ── Query 1: Revenue per 1-minute tumbling window ─────────────────────────────
def start_revenue_query(base_df):
    """
    Groups purchase events into non-overlapping 1-minute tumbling windows and
    sums the revenue within each window.

    Tumbling windows are defined by window(timeCol, windowDuration).
    Each event falls into exactly one window.

    Output columns
    --------------
    window_start  start of the 1-minute bucket
    window_end    end   of the 1-minute bucket
    total_revenue sum of price for all purchases in the window
    purchase_count number of purchase events
    """
    revenue_df = (
        base_df
        # Only purchase actions generate revenue
        .filter(col("action") == "purchase")
        .groupBy(
            window(col("event_time"), REVENUE_WIN)
        )
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
        # append: emit a row only once, when the window is guaranteed complete
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/revenue_window")
        .start()
    )


# ── Query 2: Action counts in 5-minute sliding windows ────────────────────────
def start_action_counts_query(base_df):
    """
    Groups view and purchase events into overlapping 5-minute sliding windows
    (one new window opens every minute), broken down by category and action.

    Sliding windows are defined by window(timeCol, windowDuration, slideDuration).
    Each event falls into (windowDuration / slideDuration) = 5 windows.

    Output columns
    --------------
    window_start  start of the 5-minute sliding bucket
    window_end    end   of the 5-minute sliding bucket
    category      product category (e.g. Electronics, Clothing)
    action        "view" or "purchase"
    event_count   number of events in this window / category / action cell
    """
    activity_df = (
        base_df
        # Restrict to the two actions the metric cares about
        .filter(col("action").isin("view", "purchase"))
        .groupBy(
            window(col("event_time"), ACTIVITY_WIN, ACTIVITY_SLIDE),
            col("category"),
            col("action"),
        )
        .agg(
            count("*").alias("event_count"),
        )
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
        .format("console")
        .option("truncate", "false")
        .option("numRows", "40")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/action_window")
        .start()
    )


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 62)
    print("  ecom-windowed-aggregations — Day 4")
    print("=" * 62)
    print(f"  Kafka           : {KAFKA_BOOTSTRAP}  →  {TOPIC}")
    print(f"  Watermark       : {WATERMARK}")
    print(f"  Revenue window  : tumbling {REVENUE_WIN}")
    print(f"  Activity window : sliding {ACTIVITY_WIN} / slide every {ACTIVITY_SLIDE}")
    print(f"  Trigger         : every {TRIGGER_SECS}s")
    print("=" * 62 + "\n")

    base_df = read_base_stream(spark)

    # Print schema once before starting queries
    print("Base stream schema (post-parse, post-watermark):")
    base_df.printSchema()

    q1 = start_revenue_query(base_df)
    q2 = start_action_counts_query(base_df)

    print(f"[INFO] Query 1 started  →  {q1.name}  ({q1.id})")
    print(f"[INFO] Query 2 started  →  {q2.name}  ({q2.id})")
    print(
        f"\n[NOTE] Output mode is 'append': rows appear only after the watermark\n"
        f"       advances past window_end.  With WATERMARK_DURATION={WATERMARK},\n"
        f"       expect the first revenue row ~{WATERMARK} after the first purchase.\n"
        f"       Lower WATERMARK_DURATION in .env for faster dev feedback.\n"
    )

    # Block until any query fails or is cancelled (Ctrl-C / SIGTERM)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
