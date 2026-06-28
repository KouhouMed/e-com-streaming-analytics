"""
Day 2 — Clickstream event producer.

Continuously generates synthetic e-commerce clickstream events with Faker and
publishes them as JSON to a Kafka topic.  All tunables come from environment
variables (see .env.example).

Event schema
------------
event_id   : UUIDv4  — deduplication key
timestamp  : ISO-8601 UTC
user_id    : "usr_<4-letter><4-digit>" — stable across a browsing session
session_id : "sess_<12-hex>" — unique per browser session
product_id : "<product-slug>_<4-digit-sku>"
category   : one of eight e-commerce verticals
action     : view | add_to_cart | purchase  (60 / 25 / 15 % split)
price      : float [4.99, 1299.99]
page_url   : /<category>/<product-slug>
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from faker import Faker

# ── Configuration (all overridable via env / docker-compose) ──────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "ecom.clickstream")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
MAX_RETRIES       = int(os.getenv("KAFKA_STARTUP_RETRIES", "10"))
RETRY_BASE_DELAY  = float(os.getenv("KAFKA_STARTUP_RETRY_DELAY", "3.0"))

fake = Faker()
Faker.seed(0)  # reproducible names/paths within a single run

# ── Static product catalogue ──────────────────────────────────────────────────
CATALOG: dict[str, list[str]] = {
    "Electronics":   ["laptop", "smartphone", "tablet", "headphones", "smartwatch"],
    "Clothing":      ["t-shirt", "jeans", "sneakers", "jacket", "dress"],
    "Books":         ["novel", "biography", "cookbook", "textbook", "comic"],
    "Home & Garden": ["lamp", "sofa", "plant", "rug", "coffee-maker"],
    "Sports":        ["yoga-mat", "bicycle", "dumbbells", "tennis-racket", "running-shoes"],
    "Beauty":        ["moisturizer", "perfume", "lipstick", "shampoo", "serum"],
    "Toys":          ["lego-set", "puzzle", "board-game", "remote-car", "doll"],
    "Food":          ["coffee-beans", "olive-oil", "spice-kit", "dark-chocolate", "herbal-tea"],
}
CATEGORIES = list(CATALOG.keys())

# Funnel: most events are views, fewer reach purchase
ACTIONS  = ["view", "add_to_cart", "purchase"]
WEIGHTS  = [0.60,   0.25,          0.15]


# ── Event factory ─────────────────────────────────────────────────────────────
def make_event() -> dict:
    category = random.choice(CATEGORIES)
    product  = random.choice(CATALOG[category])
    slug     = category.lower().replace(" & ", "-").replace(" ", "-")
    return {
        "event_id":   str(uuid.uuid4()),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "user_id":    f"usr_{fake.bothify(text='????####')}",
        "session_id": f"sess_{uuid.uuid4().hex[:12]}",
        "product_id": f"{product}_{random.randint(1, 500):04d}",
        "category":   category,
        "action":     random.choices(ACTIONS, weights=WEIGHTS, k=1)[0],
        "price":      round(random.uniform(4.99, 1299.99), 2),
        "page_url":   f"/{slug}/{product}",
    }


# ── Kafka connectivity ────────────────────────────────────────────────────────
def wait_for_kafka() -> None:
    """
    Poll Kafka with AdminClient until the broker is reachable.
    Uses capped exponential back-off so the container starts cleanly even if
    Kafka is still booting.
    """
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            admin.list_topics(timeout=5)
            print(f"[INFO] Kafka is ready at {BOOTSTRAP_SERVERS}")
            return
        except KafkaException as exc:
            delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), 60)
            print(
                f"[WARN] Kafka not ready (attempt {attempt}/{MAX_RETRIES}): {exc}. "
                f"Retrying in {delay:.0f}s …",
                file=sys.stderr,
            )
            time.sleep(delay)

    print("[FATAL] Kafka unreachable after all retries. Exiting.", file=sys.stderr)
    sys.exit(1)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id":         "ecom-clickstream-producer",
        # Reliability
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  500,
        # Micro-batching for throughput (trade <20 ms latency for fewer requests)
        "linger.ms":         20,
        "compression.type":  "lz4",
    })


def on_delivery(err, msg) -> None:
    """Delivery callback — logs errors; successes are silent to avoid log spam."""
    if err:
        print(f"[ERROR] Delivery failed: {err}", file=sys.stderr)


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    wait_for_kafka()
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND

    # Honour Ctrl-C and Docker stop (SIGTERM) cleanly
    running = True
    def _stop(sig, _frame):
        nonlocal running
        print(f"\n[INFO] Signal {sig} received — draining queue …")
        running = False

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(
        f"[INFO] Streaming to topic '{TOPIC}' "
        f"at {EVENTS_PER_SECOND} events/s  (Ctrl-C to stop)"
    )

    events_sent = 0
    while running:
        event = make_event()
        try:
            producer.produce(
                topic=TOPIC,
                # Partition by user_id so events for the same user land on the
                # same partition — preserves ordering within a session.
                key=event["user_id"].encode(),
                value=json.dumps(event).encode(),
                callback=on_delivery,
            )
            # poll(0) is non-blocking; it triggers any pending delivery callbacks.
            producer.poll(0)
            events_sent += 1

            if events_sent % 100 == 0:
                print(f"[INFO] {events_sent} events produced …")

        except BufferError:
            # Internal librdkafka queue is full — drain before proceeding.
            print("[WARN] Producer buffer full — flushing …", file=sys.stderr)
            producer.flush(timeout=10)

        except KafkaException as exc:
            # Transient broker error (e.g. leader election) — wait and retry.
            print(f"[ERROR] produce() raised: {exc}", file=sys.stderr)
            time.sleep(2)

        time.sleep(interval)

    # Flush any buffered messages before the process exits.
    remaining = producer.flush(timeout=30)
    if remaining:
        print(f"[WARN] {remaining} message(s) not confirmed by broker.", file=sys.stderr)

    print(f"[INFO] Producer shut down cleanly. Total events sent: {events_sent}")


if __name__ == "__main__":
    main()
