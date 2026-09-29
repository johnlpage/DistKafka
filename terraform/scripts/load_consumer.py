#!/usr/bin/env python3
"""
Load-test consumer: consumes change-stream documents (produced via the
MongoDB Kafka source connector) that were written by load_producer.py,
filters them by run_id, and writes a receipt log CSV for later latency
analysis (see compute_load_stats.py).

Each output row also carries `change_wall_ms` - the server-side
`wallTime` from the change event itself (the moment the write became
majority-committed/visible), extracted directly from the same Kafka
message. This lets compute_load_stats.py split total latency into two
honest phases: write -> visible (Mongo-side), and visible -> received
(Kafka Connect + broker + this consumer), instead of the flawed
approach of subtracting the client's full insert_one() round-trip time
(which includes time *after* the write was already visible - the ack
traveling back to the client - that has nothing to do with Kafka).

Uses a fresh consumer group per run plus auto_offset_reset="latest", and
signals a --ready-file only once it actually has a partition assignment
- this lets the orchestrator (run_load_test.sh) be certain the consumer
is subscribed *before* starting the producer, so no early messages are
missed.

Stops once it has received --expected matching messages, or after
--idle-timeout seconds with no matching messages (whichever comes
first) - the idle timeout exists purely as a safety net so the script
can't hang forever if some messages are lost.

Usage:
  python3 load_consumer.py --run-id <id> --expected 20000 \
      [--csv kafka_results.csv] [--ready-file /tmp/ready] [--idle-timeout 30]

Requires:
  kafka-python

Environment variables (from .env):
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
"""

import os
import sys
import csv
import json
import time
import argparse

try:
    from kafka import KafkaConsumer
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip3 install kafka-python")
    sys.exit(1)


def load_env(env_path="/home/ec2-user/.env"):
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    os.environ.setdefault(key, value)


def decode_event(raw):
    """Decode a (possibly double-JSON-encoded) change-event value and
    return (fullDocument dict, wall_time_ms) or (None, None) if it can't
    be parsed.

    The mongo-kafka connector's output.format.per-operation=true setting
    serializes the whole change event as a JSON *string*; combined with
    JsonConverter that means the actual Kafka record value is JSON text
    containing JSON text. A single json.loads() only unwraps the outer
    layer and yields a str, not a dict - so unwrap once more when that
    happens.

    The change event's `wallTime` field is the server's own wall-clock
    timestamp (epoch ms) of when the operation became visible - i.e.
    majority-committed. That's a strictly better anchor than trying to
    derive visibility time from the client's insert_one() round-trip
    time, since part of that round trip (the ack traveling back to the
    client) happens *after* the write is already visible - see
    load_producer.py's module docstring and compute_load_stats.py.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return None, None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return None, None
    if not isinstance(payload, dict):
        return None, None

    wall_time_ms = None
    wall_time = payload.get("wallTime")
    if isinstance(wall_time, dict):
        wall_time_ms = wall_time.get("$date")

    doc = payload.get("fullDocument", payload)
    if not isinstance(doc, dict):
        return None, None
    return doc, wall_time_ms


def main():
    parser = argparse.ArgumentParser(description="Load-test consumer")
    parser.add_argument("--run-id", required=True, help="Must match load_producer.py's --run-id")
    parser.add_argument("--expected", type=int, required=True, help="Number of messages to wait for")
    parser.add_argument("--csv", default="/home/ec2-user/kafka_results.csv",
                         help="Path to write the receipt log")
    parser.add_argument("--ready-file", default=None,
                         help="Touched once the consumer has a partition assignment")
    parser.add_argument("--idle-timeout", type=float, default=30.0,
                         help="Stop after this many seconds with no matching messages")
    parser.add_argument("--progress-interval", type=float, default=2.0)
    args = parser.parse_args()

    load_env()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "bank.payments")

    group_id = f"load-test-{args.run_id}"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=group_id,
        consumer_timeout_ms=1000,
    )

    # Wait for partition assignment before signalling ready, so the
    # producer can't start until we're actually subscribed.
    deadline = time.time() + 30
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(timeout_ms=200)

    if not consumer.assignment():
        print("ERROR: consumer never got a partition assignment", file=sys.stderr)
        sys.exit(1)

    if args.ready_file:
        with open(args.ready_file, "w") as f:
            f.write("ready\n")

    print(f"Client: {os.environ.get('CLIENT_LABEL', 'unknown')}")
    print(f"Consumer ready on topic '{topic}', group '{group_id}'. "
          f"Waiting for {args.expected} messages with run_id={args.run_id}.", flush=True)

    received = 0
    start = time.time()
    last_print = start
    last_matched = start
    stopped_early = False

    with open(args.csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "run_id", "mongo_id", "write_ts_ms", "change_wall_ms", "receipt_ts_ms"])

        while received < args.expected:
            if time.time() - last_matched > args.idle_timeout:
                stopped_early = True
                break

            for msg in consumer:
                # wall-clock, to match write_ts_ms and the change event's
                # own server-side wallTime (see load_producer.py's module
                # docstring and decode_event() above for why)
                receipt_ts_ms = time.time() * 1000.0
                doc, wall_time_ms = decode_event(msg.value)
                if doc is None or doc.get("run_id") != args.run_id:
                    continue

                seq = doc.get("seq")
                write_ts_ms = doc.get("write_ts_ms")
                mongo_id = doc.get("_id")
                writer.writerow([
                    seq, args.run_id, mongo_id, write_ts_ms,
                    wall_time_ms, f"{receipt_ts_ms:.3f}",
                ])
                f.flush()

                received += 1
                last_matched = time.time()

                now = time.time()
                if now - last_print >= args.progress_interval:
                    elapsed = now - start
                    print(f"[t+{elapsed:7.1f}s] received {received}/{args.expected}", flush=True)
                    last_print = now

                if received >= args.expected:
                    break
            else:
                # inner for-loop exhausted its consumer_timeout_ms tick
                # without a `break` - fall through to outer while check
                continue
            break  # inner for-loop hit `break` (expected count reached)

    elapsed = time.time() - start
    status = "STOPPED EARLY (idle timeout)" if stopped_early else "DONE"
    print(f"[t+{elapsed:7.1f}s] received {received}/{args.expected} - {status}", flush=True)
    print(f"Wrote receipt log to {args.csv}")

    if stopped_early:
        sys.exit(2)


if __name__ == "__main__":
    main()
