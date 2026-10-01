#!/usr/bin/env python3
"""Benchmark the customer four-document bulk write through one MONGO_URI.

This deliberately keeps the customer write shape while using the same simple
connection model as the Terraform producers:

  MONGO_URI -> one shared MongoClient -> bank database

Each bulk write inserts one task, one task outbox record, one account, and one
account outbox record. The operations use explicit namespaces so one
MongoClient.bulk_write() call can target all three collections. This is not a
multi-document transaction: a partial bulk write is possible if an operation
fails after earlier operations have succeeded. The benchmark uses unique IDs,
so benchmark documents are deleted after the timing run.
"""

import argparse
import csv
import os
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import pymongo
    from pymongo import MongoClient, WriteConcern
    from pymongo.errors import PyMongoError
except ImportError:
    print("ERROR: pymongo is not installed", file=sys.stderr)
    sys.exit(1)


DB_NAME = "bank"


def load_env(env_path="/home/ec2-user/.env"):
    if not os.path.exists(env_path):
        return
    with open(env_path) as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                os.environ.setdefault(key.strip(), value)


def percentile(values, percentage):
    if not values:
        return 0.0
    values = sorted(values)
    index = min(len(values) - 1, max(0, int((len(values) - 1) * percentage / 100)))
    return values[index]


def make_flow(run_id, sequence):
    suffix = f"{run_id}-{sequence}-{uuid.uuid4().hex}"
    task_id = f"task-{suffix}"
    account_id = f"account-{suffix}"
    task_outbox_id = f"task-outbox-{suffix}"
    account_outbox_id = f"account-outbox-{suffix}"

    return {
        "task": {
            "_id": task_id,
            "cin": f"cin-{suffix}",
            "type": "BENCHMARK",
            "status": "PENDING",
            "request_payload": {"benchmark": True, "sequence": sequence},
        },
        "account": {
            "_id": account_id,
            "cin": f"cin-{suffix}",
            "sort_code": "000000",
            "account_number": f"acct-{suffix}",
            "name": "DistKafka transaction benchmark",
            "currency": "GBP",
            "status": "ACTIVE",
            "brand": "benchmark",
            "initiated_by": "anastasia_producer_single_uri.py",
        },
        "task_outbox": {
            "_id": task_outbox_id,
            "aggregate_type": "task",
            "aggregate_id": task_id,
            "event_type": "TASK_CREATED",
            "payload": {"task_id": task_id},
        },
        "account_outbox": {
            "_id": account_outbox_id,
            "aggregate_type": "account",
            "aggregate_id": account_id,
            "event_type": "ACCOUNT_CREATED",
            "payload": {"account_id": account_id},
        },
    }


def insert_flow(client, flow, timeout_s):
    start = time.perf_counter()
    now = datetime.now(timezone.utc)
    task = flow["task"]
    account = flow["account"]
    task_outbox = flow["task_outbox"]
    account_outbox = flow["account_outbox"]

    models = [
        pymongo.InsertOne(
            {
                **task,
                "created_date": now,
                "modified_date": now,
            },
            namespace=f"{DB_NAME}.tasks",
        ),
        pymongo.InsertOne(
            {
                **task_outbox,
                "status": "PENDING",
                "created_date": now,
            },
            namespace=f"{DB_NAME}.outbox",
        ),
        pymongo.InsertOne(
            {
                **account,
                "created_date": now,
                "modified_date": now,
            },
            namespace=f"{DB_NAME}.modular_accounts",
        ),
        pymongo.InsertOne(
            {
                **account_outbox,
                "status": "PENDING",
                "created_date": now,
            },
            namespace=f"{DB_NAME}.outbox",
        ),
    ]

    with pymongo.timeout(timeout_s):
        client.bulk_write(
            models,
            ordered=True,
            write_concern=WriteConcern(w="majority"),
        )

    return (time.perf_counter() - start) * 1000.0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--threads", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="Per-bulk-write client timeout in seconds")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--log", default=None,
                        help="Optional CSV path for per-transaction latency")
    args = parser.parse_args()

    if args.count < 1 or args.threads < 1:
        parser.error("--count and --threads must be positive")

    load_env()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI is not set", file=sys.stderr)
        sys.exit(1)

    run_id = args.run_id or str(int(time.time() * 1000))
    client = MongoClient(
        uri,
        maxPoolSize=max(args.threads * 2, 40),
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        serverSelectionTimeoutMS=30000,
        retryWrites=True,
        retryReads=True,
        w="majority",
    )
    benchmark_ids = {"tasks": [], "accounts": [], "outbox": []}
    flows = []
    for sequence in range(1, args.count + 1):
        flow = make_flow(run_id, sequence)
        flows.append(flow)
        benchmark_ids["tasks"].append(flow["task"]["_id"])
        benchmark_ids["accounts"].append(flow["account"]["_id"])
        benchmark_ids["outbox"].extend([
            flow["task_outbox"]["_id"], flow["account_outbox"]["_id"],
        ])

    print(f"Database: {DB_NAME}")
    print(f"Bulk writes: {args.count}")
    print("Documents per bulk write: 4")
    print(f"Threads: {args.threads}")
    print("Connection: MONGO_URI")

    latencies = []
    failures = []
    started = time.perf_counter()
    try:
        # Force initial server selection before timing the transaction run.
        client.admin.command("ping")
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [
                executor.submit(insert_flow, client, flow, args.timeout)
                for flow in flows
            ]
            for sequence, future in enumerate(as_completed(futures), 1):
                try:
                    latencies.append(future.result())
                except Exception as exc:  # report the specific failed transaction
                    failures.append((sequence, exc))
                if sequence % 100 == 0 or sequence == args.count:
                    print(f"Completed {sequence}/{args.count}", flush=True)
    finally:
        try:
            db = client[DB_NAME]
            db.tasks.delete_many({"_id": {"$in": benchmark_ids["tasks"]}})
            db.modular_accounts.delete_many({"_id": {"$in": benchmark_ids["accounts"]}})
            db.outbox.delete_many({"_id": {"$in": benchmark_ids["outbox"]}})
        finally:
            client.close()

    elapsed = time.perf_counter() - started
    throughput = len(latencies) / elapsed if elapsed else 0.0
    print("\nBulk-write benchmark")
    print(f"Successful bulk writes: {len(latencies)}/{args.count}")
    print(f"Failed bulk writes: {len(failures)}")
    print(f"Wall time: {elapsed:.3f}s")
    print(f"Throughput: {throughput:.2f} bulk writes/s")
    if latencies:
        print(f"Latency mean: {statistics.fmean(latencies):.2f}ms")
        print(f"Latency p50: {percentile(latencies, 50):.2f}ms")
        print(f"Latency p95: {percentile(latencies, 95):.2f}ms")
        print(f"Latency p99: {percentile(latencies, 99):.2f}ms")
        print(f"Latency max: {max(latencies):.2f}ms")
    if failures:
        for sequence, error in failures[:5]:
            print(f"Failure {sequence}: {type(error).__name__}: {error}", file=sys.stderr)

    if args.log:
        with open(args.log, "w", newline="") as log_file:
            writer = csv.writer(log_file)
            writer.writerow(["latency_ms"])
            writer.writerows([[latency] for latency in latencies])

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
