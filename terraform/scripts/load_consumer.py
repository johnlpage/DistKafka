#!/usr/bin/env python3
"""
Load-test consumer: consumes change-stream documents (produced via the
MongoDB Kafka source connector watching the `tasks` collection) for the
task document written by load_producer.py's four-document transaction
(task/task-outbox/account/account-outbox), filters them by run_id, and
writes a receipt log CSV for later latency analysis (see
compute_load_stats.py). Only the task document is observed here - the
other three documents from the same transaction land in different
collections not watched by the connector - so the received count still
matches one row per transaction, same as the old single-document shape.

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

Runs until the orchestrator creates --stop-file. Receiving --expected
unique matching messages is reported, but never stops the consumer. This
keeps shutdown under explicit orchestrator control rather than inferring it
from an idle period or message count.

Also tracks the longest gap between consecutive messages seen from the
Kafka iterator (from the first message onward) and reports it at the
end. This script's own MongoClient involvement is zero - it only talks
to the local Kafka broker - but the MongoDB Kafka Source Connector's own
MongoClient (watching the change stream directly against mongos) stops
publishing to the topic for as long as ITS connection to mongos is
down. So during a mongos failover drill (whether triggered via
toggle-mongos-block.sh's network-level block, or Atlas's own built-in
resilience/fault-injection testing feature), this gap is an accurate
downstream proxy for "how long did it take the
connector to detect its mongos was down, reconnect, and resume the
change stream" - no data is lost (the connector resumes from its saved
resume token), it's purely delayed.

Usage:
  python3 load_consumer.py --run-id <id> --expected 20000 \
      [--csv kafka_results.csv] [--ready-file /tmp/ready] \
      --stop-file /tmp/stop-consumer

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
    parser.add_argument("--expected", type=int, required=True,
                        help="Expected message count used for progress reporting")
    parser.add_argument("--csv", default="/home/ec2-user/kafka_results.csv",
                         help="Path to write the receipt log")
    parser.add_argument("--ready-file", default=None,
                         help="Touched once the consumer has a partition assignment")
    parser.add_argument("--stop-file", required=True,
                         help="Exit only after this explicit stop file is created")
    parser.add_argument("--progress-interval", type=float, default=2.0)
    args = parser.parse_args()

    load_env()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "bank.tasks")

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

    received_seqs = set()
    start = time.time()
    last_print = start
    expected_reported = False
    stopped_early = False

    # Longest gap between consecutive messages seen from the Kafka
    # iterator, starting from the first message onward (nothing to
    # compare the very first message against). Tracked across EVERY
    # message the iterator yields, including ones later filtered out
    # below (wrong run_id, duplicate seq) - a filtered-out message
    # still proves the topic wasn't empty at that moment, so excluding
    # it would make an unrelated-but-real gap look artificially larger.
    first_msg_time = None
    last_msg_time = None
    max_gap_ms = 0.0

    with open(args.csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "run_id", "mongo_id", "write_ts_ms", "change_wall_ms", "receipt_ts_ms"])

        while not os.path.exists(args.stop_file):
            for msg in consumer:
                now = time.time()
                if first_msg_time is None:
                    first_msg_time = now
                else:
                    gap_ms = (now - last_msg_time) * 1000.0
                    if gap_ms > max_gap_ms:
                        max_gap_ms = gap_ms
                last_msg_time = now

                # wall-clock, to match write_ts_ms and the change event's
                # own server-side wallTime (see load_producer.py's module
                # docstring and decode_event() above for why)
                receipt_ts_ms = now * 1000.0
                doc, wall_time_ms = decode_event(msg.value)
                if doc is None or doc.get("run_id") != args.run_id:
                    continue

                seq = doc.get("seq")
                if seq is None or seq in received_seqs:
                    continue
                write_ts_ms = doc.get("write_ts_ms")
                mongo_id = doc.get("_id")
                writer.writerow([
                    seq, args.run_id, mongo_id, write_ts_ms,
                    wall_time_ms, f"{receipt_ts_ms:.3f}",
                ])
                f.flush()

                received_seqs.add(seq)
                now = time.time()
                if now - last_print >= args.progress_interval:
                    elapsed = now - start
                    print(f"[t+{elapsed:7.1f}s] received {len(received_seqs)}/{args.expected}", flush=True)
                    last_print = now

                if not expected_reported and len(received_seqs) >= args.expected:
                    expected_reported = True
                    print(
                        f"Reached expected count {args.expected}; "
                        "continuing until explicit stop signal.",
                        flush=True,
                    )
            else:
                # inner for-loop exhausted its consumer_timeout_ms tick
                # without a `break` - fall through to outer while check
                continue
            # The consumer timeout ends this iterator periodically so the
            # explicit stop-file check above is responsive.

    elapsed = time.time() - start
    stopped_early = len(received_seqs) < args.expected
    status = "STOPPED BEFORE EXPECTED" if stopped_early else "STOPPED BY EXPLICIT SIGNAL"
    print(f"[t+{elapsed:7.1f}s] received {len(received_seqs)}/{args.expected} - {status}", flush=True)
    if first_msg_time is None or last_msg_time == first_msg_time:
        print("Max gap between consumed messages: n/a (fewer than 2 messages received)")
    else:
        print(f"Max gap between consumed messages: {max_gap_ms:.0f}ms")
    print(f"Wrote receipt log to {args.csv}")

    if stopped_early:
        sys.exit(2)


if __name__ == "__main__":
    main()
