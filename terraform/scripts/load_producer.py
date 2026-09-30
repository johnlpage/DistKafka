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
    import pymongo
    from pymongo import MongoClient, WriteConcern
    from pymongo.errors import DuplicateKeyError, PyMongoError
    from bson import ObjectId
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


def insert_one_doc(collection, hostname, run_id, seq, timeout_s=5.0, max_retries=None):
    """Insert a single document, timing the whole (possibly retried)
    attempt. Runs in a worker thread; pymongo's MongoClient is
    thread-safe and pools connections internally, so sharing
    `collection` across threads is safe.

    Uses writeConcern={w:"majority"} (set by the caller on `collection`
    via `.with_options(write_concern=WriteConcern(w="majority",
    wtimeout=...))`) plus a CLIENT-side deadline (PyMongo's CSOT
    `pymongo.timeout()`). These cover two different failure modes:

    1. `wtimeout` - a SERVER-side bound: once the primary has actually
       received and executed the command, this is how long IT waits
       for replica acks before giving up and returning a clean
       WriteConcernError. This is the common, "healthy but replicating
       slowly" case, and firing this gives the nicest, fastest,
       most-informative failure.

    2. CSOT (`pymongo.timeout()`) - a CLIENT-side bound that fires
       regardless of what's happening server-side or even at the
       socket level. This is what actually saves us in the failure
       mode observed during DistKafka's regional-failover testing:
       mongos's lightweight replica-set monitor correctly detected the
       newly-elected primary as healthy within seconds, but its
       separate execution-pool connection to that same primary was
       wedged (killed mid-flight by the simulated outage, no clean TCP
       FIN/RST) - a write sent over it just hung with no response at
       all, so `wtimeout` never even started its clock. A plain
       client-side socket read in that situation blocks until the OS's
       own TCP retransmission logic eventually gives up (tens of
       minutes, not configurable here) - confirmed live: pointing
       PyMongo at a deliberately blackholed address, `pymongo.timeout(5)`
       reliably raised after exactly 5.00s regardless.

    Both are set to the same `timeout_s` for simplicity - whichever
    fires first (usually wtimeout, when the primary is reachable at
    all) triggers the same retry path below.

    The document keeps the SAME _id across retries (rather than a
    fresh one per attempt): if an earlier attempt's write actually
    made it to the primary but we just timed out waiting to hear
    back, the retry hits a DuplicateKeyError instead of silently
    inserting a second copy of the same logical document - at which
    point we know the original attempt succeeded and stop.

    Returns (seq, mongo_id, write_ts_ms, insert_ms, attempts).
    mongo_id is None in the (rare) duplicate-key case, since we don't
    have the prior attempt's server-assigned result to hand back -
    the caller should treat that the same as success for counting
    purposes (the doc's _id, doc["_id"], is what's authoritative).
    """
    write_ts_ms = time.time() * 1000.0
    doc = {
        "_id": ObjectId(),
        "seq": seq,
        "run_id": run_id,
        "write_ts_ms": write_ts_ms,
        "host_created": hostname,
        "source": "load_producer.py",
    }

    attempt = 0
    t_start = time.perf_counter()

    while True:
        attempt += 1
        try:
            with pymongo.timeout(timeout_s):
                result = collection.insert_one(doc)
            insert_ms = (time.perf_counter() - t_start) * 1000.0
            return seq, str(result.inserted_id), write_ts_ms, insert_ms, attempt

        except DuplicateKeyError:
            insert_ms = (time.perf_counter() - t_start) * 1000.0
            return seq, str(doc["_id"]), write_ts_ms, insert_ms, attempt

        except PyMongoError:
            # Covers CSOT deadline expiry (the common case here) as
            # well as any other retryable-ish PyMongo error - either
            # way we don't yet know if the write landed, so retry
            # (with the same _id) rather than give up or duplicate.
            if max_retries is not None and attempt >= max_retries:
                raise
            # No sleep/backoff by design: the CSOT deadline itself
            # already paces retries.
            continue


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
    parser.add_argument("--write-timeout", type=float, default=5.0,
                         help="Client-side (CSOT) seconds to wait per insert attempt "
                              "before retrying (default: 5.0)")
    parser.add_argument("--max-retries", type=int, default=None,
                         help="Max retry attempts per document before giving up "
                              "(default: unlimited)")
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
    collection = client[db_name][collection_name].with_options(
        write_concern=WriteConcern(w="majority", wtimeout=int(args.write_timeout * 1000))
    )

    print(f"RUN_ID={run_id}")
    print(f"Client: {os.environ.get('CLIENT_LABEL', 'unknown')}")
    print(f"Inserting {args.count} documents into {db_name}.{collection_name} "
          f"with {args.concurrency} concurrent workers ...", flush=True)

    with open(args.log, "w", newline="") as logf:
        writer = csv.writer(logf)
        writer.writerow(["seq", "run_id", "mongo_id", "write_ts_ms", "insert_ms", "attempts"])

        start = time.time()
        last_print = start
        produced = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [
                pool.submit(
                    insert_one_doc, collection, hostname, run_id, seq,
                    timeout_s=args.write_timeout, max_retries=args.max_retries,
                )
                for seq in range(1, args.count + 1)
            ]

            for future in as_completed(futures):
                try:
                    seq, mongo_id, write_ts_ms, insert_ms, attempts = future.result()
                except Exception as exc:
                    failed += 1
                    print(f"ERROR: insert failed: {exc}", file=sys.stderr)
                    continue

                writer.writerow([seq, run_id, mongo_id, f"{write_ts_ms:.3f}", f"{insert_ms:.3f}", attempts])
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
