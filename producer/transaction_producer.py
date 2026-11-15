"""Continuously publish CSV transactions without sorting input rows."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


def read_rows(path: Path) -> list[dict[str, str]]:
    """Read the CSV in file order and retain every original column/value."""
    with path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames or "transaction_id" not in reader.fieldnames:
            raise ValueError(f"{path} must contain a transaction_id column")
        return [dict(row) for row in reader]


def delivery_report(record, error: Exception | None) -> None:
    if error:
        logging.error("Delivery failed for transaction_id=%s: %s", record.get("transaction_id"), error)
    else:
        logging.info("Delivered topic=%s partition=%s offset=%s", record.topic, record.partition, record.offset)


def disorder_rows(rows: list[dict[str, str]], minimum_seconds: int = 0):
    """Reverse bounded sequential blocks to test event-time disorder without sorting the input.

    A threshold below 60 seconds uses blocks spanning 30..59 seconds. A threshold of
    60+ seconds uses blocks spanning 61..120 seconds. Blocks with larger timestamp
    gaps are passed through unchanged, which avoids claiming a disorder bound the
    data cannot provide.
    """
    if minimum_seconds <= 0:
        yield from rows
        return
    max_span = 59 if minimum_seconds < 60 else 120
    buffer: list[dict[str, str]] = []
    stamps: list[datetime] = []
    for row in rows:
        stamp = datetime.fromisoformat(row["event_timestamp"].replace("Z", "+00:00"))
        candidate = stamps + [stamp]
        span = (max(candidate) - min(candidate)).total_seconds()
        if buffer and span > max_span:
            yield from (reversed(buffer) if minimum_seconds <= (max(stamps) - min(stamps)).total_seconds() <= max_span else buffer)
            buffer, stamps = [], []
        buffer.append(row)
        stamps.append(stamp)
        span = (max(stamps) - min(stamps)).total_seconds()
        if span > max_span:
            # A single large gap cannot form a bounded reorder block.
            yield from buffer
            buffer, stamps = [], []
        elif span >= minimum_seconds:
            yield from reversed(buffer)
            buffer, stamps = [], []
    if buffer:
        span = (max(stamps) - min(stamps)).total_seconds()
        yield from (reversed(buffer) if minimum_seconds <= span <= max_span else buffer)


def publish(rows: list[dict[str, str]], bootstrap: str, topic: str, interval: float,
            duplicate_every: int = 0, disorder_seconds: int = 0, loop: bool = True) -> None:
    """Publish input order or the explicitly selected bounded disorder test sequence."""
    if interval < 0 or duplicate_every < 0 or disorder_seconds < 0:
        raise ValueError("interval, duplicate_every and disorder_seconds must be non-negative")
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=[v.strip() for v in bootstrap.split(",")],
        client_id=os.getenv("KAFKA_CLIENT_ID", "fraud-producer"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        retries=8, acks="all", max_in_flight_requests_per_connection=1)
    try:
        pass_number = 0
        while True:
            pass_number += 1
            sequence = 0
            def send(row: dict[str, str]) -> None:
                nonlocal sequence
                sequence += 1
                future = producer.send(topic, value=row, key=row["transaction_id"].encode("utf-8"))
                future.add_callback(lambda meta: delivery_report(meta, None))
                future.add_errback(lambda err, r=row: delivery_report(r, err))
                # Produce explicit repeats only when requested by the experiment.
                if duplicate_every and sequence % duplicate_every == 0:
                    producer.send(topic, value=row, key=row["transaction_id"].encode("utf-8"))
                time.sleep(interval)
            for row in disorder_rows(rows, disorder_seconds):
                send(row)
            producer.flush(timeout=60)
            logging.info("Completed input pass %s (%s input rows)", pass_number, len(rows))
            if not loop:
                break
    finally:
        producer.close(timeout=30)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path(os.getenv("TRANSACTIONS_CSV", "data/transactions.csv")))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions-run1"))
    parser.add_argument("--interval", type=float, default=float(os.getenv("PRODUCER_INTERVAL_SECONDS", ".05")))
    parser.add_argument("--duplicate-every", type=int, default=int(os.getenv("PRODUCER_DUPLICATE_EVERY", "0")))
    parser.add_argument("--disorder-seconds", type=int, default=int(os.getenv("PRODUCER_DISORDER_SECONDS", "0")), help="reverse bounded blocks spanning at least this many event-time seconds (test mode)")
    parser.add_argument("--once", action="store_true", help="publish the CSV once; default continuously replays it")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows = read_rows(args.csv)
    if not rows:
        raise SystemExit(f"No transactions found in {args.csv}")
    publish(rows, args.bootstrap, args.topic, args.interval, args.duplicate_every, args.disorder_seconds, not args.once)


if __name__ == "__main__":
    main()
