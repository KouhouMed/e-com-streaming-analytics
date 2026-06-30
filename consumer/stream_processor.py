"""
Day 3 — Structured Streaming baseline (console sink).

Reads the raw JSON clickstream from Kafka, deserialises each record against
the known schema, prints the DataFrame schema to stdout, and streams every
micro-batch to the console for visual verification.

No persistence yet — that arrives in Day 4.
All config is read from environment variables (see .env.example).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC            = os.getenv("KAFKA_TOPIC_CLICKSTREAM",  "ecom.clickstream")
TRIGGER_SECS     = os.getenv("TRIGGER_INTERVAL_SECS",    "5")
CHECKPOINT_DIR   = os.getenv("CHECKPOINT_DIR",            "/tmp/spark-checkpoints/console")

# ── Event schema — must mirror generator.py exactly ───────────────────────────
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


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecom-clickstream-console")
        # Keep shuffle partitions small — we have 1 Kafka partition in dev
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    # Suppress INFO/DEBUG noise so the console output stays readable
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print(f"  Kafka bootstrap : {KAFKA_BOOTSTRAP}")
    print(f"  Topic           : {TOPIC}")
    print(f"  Trigger         : every {TRIGGER_SECS}s")
    print("=" * 60 + "\n")

    # ── Step 1: Read raw bytes from Kafka ─────────────────────────────────────
    # Each Kafka message arrives as binary key + binary value.
    # Kafka also provides metadata columns: topic, partition, offset, timestamp.
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        # "latest" = only process events that arrive after the job starts
        .option("startingOffsets", "latest")
        # Tolerate gaps in offsets caused by Kafka log compaction / retention
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Step 2: Decode binary columns to UTF-8 strings ────────────────────────
    string_df = raw_df.selectExpr(
        "CAST(key       AS STRING) AS user_id_key",
        "CAST(value     AS STRING) AS json_value",
        "partition",
        "offset",
        "timestamp                AS kafka_ts",
    )

    # ── Step 3: Parse the JSON payload against the known schema ───────────────
    # from_json returns a single StructType column; col("event.*") flattens it.
    parsed_df = (
        string_df
        .withColumn("event", from_json(col("json_value"), CLICKSTREAM_SCHEMA))
        .select(
            "kafka_ts",
            "partition",
            "offset",
            col("event.event_id"),
            col("event.timestamp"),
            col("event.user_id"),
            col("event.session_id"),
            col("event.product_id"),
            col("event.category"),
            col("event.action"),
            col("event.price"),
            col("event.page_url"),
        )
    )

    # ── Step 4: Print the parsed schema (runs immediately, before any data) ───
    print("=" * 60)
    print("  Parsed stream schema")
    print("=" * 60)
    parsed_df.printSchema()

    # ── Step 5: Stream every micro-batch to the console ───────────────────────
    query = (
        parsed_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .start()
    )

    print(f"[INFO] Awaiting stream data from '{TOPIC}' — Ctrl-C to stop\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
