#!/usr/bin/env python3
"""
Join load_producer.py's insert_log.csv with load_consumer.py's receipt
log (kafka_results.csv) on `seq`, and print summary statistics for:

  (a) insert time         - time spent inside insert_one() (the client's
                             full round-trip ack time)
  (b) mongo visibility    - change_wall_ms - write_ts_ms, i.e. how long
                             until the write was majority-committed and
                             visible to a change stream, per the change
                             event's own server-side `wallTime` field
  (c) kafka pipeline time - receipt_ts_ms - change_wall_ms, i.e. Kafka
                             Connect -> Kafka broker -> this consumer,
                             strictly downstream of (b) so it should
                             never legitimately be negative
  (d) create to receipt   - receipt_ts_ms - write_ts_ms, the full
                             end-to-end total (equal to (b) + (c);
                             reported explicitly for convenience)

Why not just (receipt_ts_ms - write_ts_ms) - insert_ms, i.e. subtract
the client's full insert_one() time from the total? Because part of
that client-observed time happens *after* the write is already
majority-committed and visible - specifically, the ack traveling back
across the network to the client. Kafka Connect's own path to noticing
the change can easily be faster than that return trip, so that naive
subtraction can (and did) go negative. Anchoring on the change event's
own wallTime instead of the client's round-trip time avoids this
entirely.

Usage:
  python3 compute_load_stats.py [--insert-log insert_log.csv] [--receipt-log kafka_results.csv]
      [--client-label "..."] [--run-id <id>] [--json-out results_<id>.json]
"""

import csv
import os
import json
import time
import argparse
import statistics


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


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def summarize(name, values):
    """Print the usual human-readable block AND return the same numbers
    as a dict, so callers can also serialize them (see --json-out)."""
    print(f"{name}:")
    if not values:
        print("  no data")
        return {"n": 0}
    vals = sorted(values)
    stats = {
        "n": len(vals),
        "min_ms": vals[0],
        "mean_ms": statistics.fmean(vals),
        "p95_ms": percentile(vals, 95),
        "p99_ms": percentile(vals, 99),
        "max_ms": vals[-1],
    }
    print(f"  n      = {stats['n']}")
    print(f"  min    = {stats['min_ms']:.2f} ms")
    print(f"  mean   = {stats['mean_ms']:.2f} ms")
    print(f"  p95    = {stats['p95_ms']:.2f} ms")
    print(f"  p99    = {stats['p99_ms']:.2f} ms")
    print(f"  max    = {stats['max_ms']:.2f} ms")
    return stats


def main():
    parser = argparse.ArgumentParser(description="Compute load-test latency stats")
    parser.add_argument("--insert-log", default="/home/ec2-user/insert_log.csv")
    parser.add_argument("--receipt-log", default="/home/ec2-user/kafka_results.csv")
    parser.add_argument("--client-label", default=None,
                         help="Override the printed client label instead of reading CLIENT_LABEL "
                              "from .env (needed when run somewhere with no local .env, e.g. a "
                              "laptop orchestrating multiple remote hosts)")
    parser.add_argument("--run-id", default=None,
                         help="Run ID to embed in the JSON output (see --json-out). Purely "
                              "informational/for-your-records - stats are always computed from "
                              "whatever rows are actually in the CSVs, not filtered by this.")
    parser.add_argument("--json-out", default=None,
                         help="Also write the summary as JSON to this path, in addition to the "
                              "normal screen output (e.g. results_<run_id>.json)")
    args = parser.parse_args()

    load_env()
    client_label = args.client_label or os.environ.get("CLIENT_LABEL", "unknown")

    inserts = {}  # seq -> (write_ts_ms, insert_ms)
    with open(args.insert_log, newline="") as f:
        for row in csv.DictReader(f):
            try:
                seq = int(row["seq"])
                inserts[seq] = (float(row["write_ts_ms"]), float(row["insert_ms"]))
            except (KeyError, ValueError, TypeError):
                continue

    receipts = {}  # seq -> (change_wall_ms, receipt_ts_ms)
    with open(args.receipt_log, newline="") as f:
        for row in csv.DictReader(f):
            try:
                seq = int(row["seq"])
                change_wall_ms = row.get("change_wall_ms")
                receipts[seq] = (
                    float(change_wall_ms) if change_wall_ms not in (None, "") else None,
                    float(row["receipt_ts_ms"]),
                )
            except (KeyError, ValueError, TypeError):
                continue

    sent = len(inserts)
    received = len(receipts)
    matched_seqs = sorted(set(inserts) & set(receipts))
    missing = sent - len(matched_seqs)

    insert_times = []
    mongo_visibility_times = []
    kafka_pipeline_times = []
    create_to_receipt_times = []
    missing_wall_time = 0

    for s in matched_seqs:
        write_ts_ms, insert_ms = inserts[s]
        change_wall_ms, receipt_ts_ms = receipts[s]
        insert_times.append(insert_ms)
        create_to_receipt_times.append(receipt_ts_ms - write_ts_ms)

        if change_wall_ms is None:
            missing_wall_time += 1
            continue

        mongo_visibility_times.append(change_wall_ms - write_ts_ms)
        kafka_pipeline_times.append(receipt_ts_ms - change_wall_ms)

    print("=" * 64)
    print("Load test summary")
    print(f"Client:   {client_label}")
    print("=" * 64)
    print(f"Sent:     {sent}")
    print(f"Received: {received}")
    print(f"Matched:  {len(matched_seqs)}")
    print(f"Missing:  {missing}")
    if missing_wall_time:
        print(f"(Note: {missing_wall_time} matched records had no change_wall_ms - "
              f"older receipt log format? Excluded from the visibility/pipeline stats below.)")
    print()
    insert_stats = summarize("Insert time  (client's full insert_one() round-trip)", insert_times)
    print()
    mongo_visibility_stats = summarize(
        "Mongo visibility time  (write -> change-stream visible, per server wallTime)",
        mongo_visibility_times)
    print()
    kafka_pipeline_stats = summarize(
        "Kafka pipeline time  (change-stream visible -> consumer receipt)",
        kafka_pipeline_times)
    print()
    create_to_receipt_stats = summarize(
        "Create to receipt  (full end-to-end: write -> consumer receipt)",
        create_to_receipt_times)
    print("=" * 64)

    if args.json_out:
        payload = {
            "run_id": args.run_id,
            "client_label": client_label,
            "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "insert_log": os.path.abspath(args.insert_log),
            "receipt_log": os.path.abspath(args.receipt_log),
            "sent": sent,
            "received": received,
            "matched": len(matched_seqs),
            "missing": missing,
            "missing_wall_time": missing_wall_time,
            "insert_time_ms": insert_stats,
            "mongo_visibility_time_ms": mongo_visibility_stats,
            "kafka_pipeline_time_ms": kafka_pipeline_stats,
            "create_to_receipt_time_ms": create_to_receipt_stats,
        }
        with open(args.json_out, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote JSON summary to {args.json_out}")


if __name__ == "__main__":
    main()
