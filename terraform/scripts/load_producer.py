#!/usr/bin/env python3
"""
Load-test producer: inserts N documents into MongoDB one at a time
(still one insert_one() call per document - just many of them in
flight concurrently, via a thread pool), timing each individual
insert_one() call, and logs the results to a CSV for later latency
analysis (see compute_load_stats.py).

Each document embeds:
  seq          - sequence number (1..count), used to join with the
                 consumer's receipt log
  run_id       - unique identifier for this run, so the consumer can
                 ignore any stale/leftover messages from a previous run
  write_ts_ms  - wall-clock epoch-ms timestamp (time.time()) taken
                 immediately before the insert_one() call.

                 This is deliberately wall-clock, not monotonic: the
                 point of write_ts_ms is to be compared against the
                 change event's own server-side `wallTime` field (see
                 load_consumer.py), which is an absolute epoch
                 timestamp from the Atlas cluster's clock, not ours.
                 That comparison is only valid if our local clock is
                 well NTP-synced to real time - confirmed via `chronyc
                 tracking` showing ~2 microsecond offset on these
                 hosts, so wall-clock is safe here.

                 Do NOT use `insert_ms` (the client's full round-trip
                 ack time, measured separately below) as a proxy for
                 "time until the write was visible to a change stream
                 watcher" - part of that round trip happens *after*
                 the write is already majority-committed and visible
                 (the ack traveling back across the network to the
                 client), so subtracting the whole thing over-corrects
                 and can go negative. write_ts_ms + wallTime gives the
                 real answer instead - see compute_load_stats.py.

Usage:
  python3 load_producer.py --count 20000 --run-id <id> --concurrency 20 [--log insert_log.csv]

Requires:
  pymongo

Environment variables (from .env):
  MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION
"""

import os
import sys
import csv
import time
import socket
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed. Run: pip3 install pymongo")
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
                    # .env values are quoted (KEY="value") for safe `source`-ing
                    # in bash; strip matching quotes since this parser isn't
                    # shell-aware.
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    os.environ.setdefault(key, value)


def insert_one_doc(collection, hostname, run_id, seq):
    """Insert a single document, timing just the insert_one() call.
    Runs in a worker thread; pymongo's MongoClient is thread-safe and
    pools connections internally, so sharing `collection` across
    threads is safe."""
    write_ts_ms = time.time() * 1000.0
    doc = {
        "seq": seq,
        "run_id": run_id,
        "write_ts_ms": write_ts_ms,
        "host_created": hostname,
        "source": "load_producer.py",
    }

    t0 = time.perf_counter()
    result = collection.insert_one(doc)
    insert_ms = (time.perf_counter() - t0) * 1000.0

    return seq, str(result.inserted_id), write_ts_ms, insert_ms


def main():
    parser = argparse.ArgumentParser(description="Load-test producer")
    parser.add_argument("--count", type=int, default=20000, help="Number of documents to insert")
    parser.add_argument("--run-id", default=None,
                         help="Unique run identifier embedded in each document (default: current epoch ms)")
    parser.add_argument("--concurrency", type=int, default=20,
                         help="Number of concurrent inserting threads (default: 20)")
    parser.add_argument("--log", default="/home/ec2-user/insert_log.csv",
                         help="Path to write the per-insert timing log")
    parser.add_argument("--progress-interval", type=float, default=2.0,
                         help="Seconds between progress prints")
    args = parser.parse_args()

    load_env()

    mongo_uri = os.environ.get("MONGO_URI")
    db_name = os.environ.get("MONGO_DB_NAME", "bank")
    collection_name = os.environ.get("MONGO_COLLECTION", "payments")
    hostname = socket.gethostname()

    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable not set", file=sys.stderr)
        sys.exit(1)

    run_id = args.run_id or str(int(time.time() * 1000))

    # maxPoolSize must cover the thread pool, or threads will queue up
    # waiting for a connection instead of actually running concurrently.
    client = MongoClient(mongo_uri, maxPoolSize=max(args.concurrency, 100))
    collection = client[db_name][collection_name]

    print(f"RUN_ID={run_id}")
    print(f"Client: {os.environ.get('CLIENT_LABEL', 'unknown')}")
    print(f"Inserting {args.count} documents into {db_name}.{collection_name} "
          f"with {args.concurrency} concurrent workers ...", flush=True)

    with open(args.log, "w", newline="") as logf:
        writer = csv.writer(logf)
        writer.writerow(["seq", "run_id", "mongo_id", "write_ts_ms", "insert_ms"])

        start = time.time()
        last_print = start
        produced = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [
                pool.submit(insert_one_doc, collection, hostname, run_id, seq)
                for seq in range(1, args.count + 1)
            ]

            for future in as_completed(futures):
                try:
                    seq, mongo_id, write_ts_ms, insert_ms = future.result()
                except Exception as exc:
                    failed += 1
                    print(f"ERROR: insert failed: {exc}", file=sys.stderr)
                    continue

                writer.writerow([seq, run_id, mongo_id, f"{write_ts_ms:.3f}", f"{insert_ms:.3f}"])
                produced += 1

                now = time.time()
                if now - last_print >= args.progress_interval:
                    elapsed = now - start
                    rate = produced / elapsed if elapsed > 0 else 0.0
                    print(f"[t+{elapsed:7.1f}s] produced {produced}/{args.count} ({rate:.1f}/s)", flush=True)
                    last_print = now

        elapsed = time.time() - start
        rate = produced / elapsed if elapsed > 0 else 0.0
        suffix = f" ({failed} failed)" if failed else ""
        print(f"[t+{elapsed:7.1f}s] produced {produced}/{args.count} ({rate:.1f}/s) - DONE{suffix}", flush=True)

    print(f"Wrote insert log to {args.log}")


if __name__ == "__main__":
    main()
