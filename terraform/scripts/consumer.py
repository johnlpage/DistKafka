#!/usr/bin/env python3
"""
Read from the bank.tasks Kafka topic (fed by the connector watching the
`tasks` collection - the one document per transaction that producer.py's
four-document transaction writes) and write results to a CSV file.
Columns: _id, host_created, time_created, time_retrieved

Usage:
  python3 consumer.py                  # consume from beginning, write CSV
  python3 consumer.py --tail            # follow new messages (no CSV)
  python3 consumer.py --continuous      # keep polling, append to CSV

Requires:
  kafka-python

Environment variables (from .env):
  KAFKA_BOOTSTRAP_SERVERS - Kafka broker (default: localhost:9092)
  KAFKA_TOPIC - topic to consume (default: bank.tasks)
"""

import os
import sys
import csv
import json
import signal
from datetime import datetime, timezone

try:
    from kafka import KafkaConsumer
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip3 install kafka-python")
    sys.exit(1)


CSV_FILE = "/home/ec2-user/kafka_results.csv"
FIELDNAMES = ["_id", "host_created", "time_created", "time_retrieved"]


def load_env(env_path="/home/ec2-user/.env"):
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    # .env values are quoted (KEY="value") for safe `source`-ing
                    # in bash; strip matching quotes here since this parser
                    # isn't shell-aware.
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    os.environ.setdefault(key, value)


def ensure_csv_header():
    """Write CSV header if file does not exist."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
        print(f"Created {CSV_FILE} with header", file=sys.stderr)


def process_message(msg_value, writer, f):
    """Parse a Kafka message and write to CSV."""
    time_retrieved = datetime.now(timezone.utc).isoformat()

    try:
        payload = json.loads(msg_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return

    # With output.format.per-operation=true, the mongo-kafka connector
    # serializes the whole change-event as a JSON *string* value. Combined
    # with JsonConverter, that means the actual record value is double
    # JSON-encoded (a JSON string containing JSON text), e.g.:
    #   "{\"_id\": ..., \"fullDocument\": {...}}"
    # A single json.loads() only unwraps the outer layer and yields a plain
    # str, not a dict - so unwrap once more when that happens.
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return

    # The connector outputs the full document in the payload
    doc = payload.get("fullDocument", payload) if isinstance(payload, dict) else payload

    if not isinstance(doc, dict):
        return

    doc_id = str(doc.get("_id", ""))
    host_created = doc.get("host_created", "")
    time_created = doc.get("time_created", "")

    row = {
        "_id": doc_id,
        "host_created": host_created,
        "time_created": time_created,
        "time_retrieved": time_retrieved,
    }

    writer.writerow(row)
    f.flush()
    print(json.dumps(row))


def main():
    load_env()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "bank.tasks")

    continuous = "--continuous" in sys.argv
    tail_mode = "--tail" in sys.argv

    if tail_mode:
        # Tail mode: read from end, print to stdout, no CSV
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        print(f"Tailing topic '{topic}'... (Ctrl+C to stop)", file=sys.stderr)
        for msg in consumer:
            print(json.dumps({"time_retrieved": datetime.now(timezone.utc).isoformat(),
                              "value": msg.value}))
        return

    # Default mode: consume from beginning, write CSV
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=15000,  # stop after 15s of no new messages
    )

    ensure_csv_header()

    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        count = 0
        for msg in consumer:
            process_message(msg.value, writer, f)
            count += 1

    print(f"\nDone. Consumed {count} messages. Results in {CSV_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()