#!/usr/bin/env python3
"""Traffic generator for CHView demo. Inserts realistic e-commerce events."""

import math
import os
import random
import time
import uuid

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food", "Toys", "Beauty"]
COUNTRIES = ["US", "GB", "DE", "FR", "ES", "JP", "BR", "CA", "AU", "IN"]
DEVICES = ["desktop", "mobile", "tablet"]
REFERRERS = ["google", "direct", "facebook", "twitter", "email", "instagram", "tiktok"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "refund", "search"]
EVENT_WEIGHTS = [50, 20, 15, 3, 12]  # Realistic distribution


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        secure=os.getenv("CLICKHOUSE_SECURE", "False").lower() in ("true", "1", "yes"),
    )


def dynamic_batch_size(t):
    """Compute a dynamic batch size based on elapsed time.

    Combines:
    - Sine wave for gradual variation (period ~5 minutes)
    - Periodic bursts: 3x spike for ~10s every ~2 minutes
    - Periodic quiet periods: 50% drop for ~15s every ~3 minutes
    """
    base = 200

    # Sine wave: gradual variation +/- 30%
    sine = math.sin(2 * math.pi * t / 300)  # 5-min period
    size = base * (1 + 0.3 * sine)

    # Burst: 3x spike for 10s, every ~120s
    burst_cycle = t % 120
    if burst_cycle < 10:
        size *= 3.0

    # Quiet period: 50% drop for 15s, every ~180s
    quiet_cycle = t % 180
    if 60 <= quiet_cycle < 75:
        size *= 0.5

    return max(10, int(size))


def generate_batch(size=100):
    """Generate a batch of random events."""
    rows = []
    for _ in range(size):
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        user_id = random.randint(1, 5000)
        product_id = random.randint(1, 200)
        category = random.choice(CATEGORIES)
        price = round(random.uniform(5, 500), 2)
        quantity = random.randint(1, 5) if event_type in ("purchase", "add_to_cart", "refund") else 1

        rows.append([
            str(uuid.uuid4()),
            user_id,
            f"sess-{user_id}-{random.randint(1, 20)}",
            event_type,
            product_id,
            category,
            price,
            quantity,
            random.choice(COUNTRIES),
            random.choice(DEVICES),
            random.choice(REFERRERS),
        ])
    return rows


def main():
    client = get_client()

    # Verify connection
    version = client.query("SELECT version()").first_row[0]
    print(f"Connected to ClickHouse {version}")

    interval = 1.0  # seconds between batches
    total_inserted = 0
    start_time = time.time()

    columns = [
        "event_id", "user_id", "session_id", "event_type",
        "product_id", "category", "price", "quantity",
        "country", "device", "referrer",
    ]

    print(f"Generating events with dynamic batch sizes every {interval}s — press Ctrl+C to stop\n")

    try:
        while True:
            elapsed = time.time() - start_time
            batch_size = dynamic_batch_size(elapsed)
            batch = generate_batch(batch_size)
            client.insert(
                "ecommerce.raw_events",
                batch,
                column_names=columns,
            )
            total_inserted += batch_size
            purchases = sum(1 for r in batch if r[3] == "purchase")
            print(f"  Inserted {batch_size} events ({purchases} purchases) — total: {total_inserted:,}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Total events inserted: {total_inserted:,}")


if __name__ == "__main__":
    main()
