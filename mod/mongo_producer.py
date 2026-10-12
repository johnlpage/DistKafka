"""
MongoDB Golden Source Producer

Reads pre-generated records from golden_source.json and inserts them into MongoDB.
Measures and reports insert throughput and latency.
Embeds seq, run_id, and write_ts_ms in each task document for consumer-side
latency join (see compute_load_stats.py).

Usage:
  python3 mongo_producer.py --golden golden_source.json --threads 10 [--run-id <id>] [--log insert_log.csv]
"""

import argparse
import csv
import json
import os
import sys
import time
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_env(env_path=None):
    if env_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for candidate in (
            os.path.join(script_dir, '.env'),
            '/home/ec2-user/.env',
        ):
            if os.path.exists(candidate):
                env_path = candidate
                break
    if env_path is None or not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                os.environ.setdefault(key, value)


load_env()

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB_NAME", "modular-accounts")

if not MONGO_URI:
    print("ERROR: MONGO_URI not set. Create a .env file with MONGO_URI=<your-connection-string>")
    sys.exit(1)


@dataclass
class Stats:
    inserted: int = 0
    errors: int = 0
    latencies_ms: list = field(default_factory=list)
    timestamps: dict = field(default_factory=dict)  # outbox_id -> {started: ms, completed: ms}
    failed_ids: list = field(default_factory=list)  # outbox_ids that failed to persist
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, duration_ms, outbox_ids=None, started_ms=None, completed_ms=None):
        with self._lock:
            self.inserted += 1
            self.latencies_ms.append(duration_ms)
            if outbox_ids and started_ms and completed_ms:
                for oid in outbox_ids:
                    self.timestamps[oid] = {"started": started_ms, "completed": completed_ms}

    def add_error(self, outbox_ids=None, reason=None):
        with self._lock:
            self.errors += 1
            if outbox_ids:
                self.failed_ids.append({
                    "outbox_ids": outbox_ids,
                    "reason": str(reason) if reason else "unknown",
                    "timestamp_ms": int(time.time() * 1000),
                })


def insert_flow(flow, stats, client, seq, run_id, log_writer):
    """Insert one flow (task + outbox + account + outbox) into MongoDB using a transaction."""
    started_ms = int(time.time() * 1000)  # Capture BEFORE insert
    t_start = time.perf_counter()
    write_ts_ms = time.time() * 1000.0
    outbox_ids = [flow['task_outbox']['outbox_id'], flow['account_outbox']['outbox_id']]
    task_id = flow['task']['task_id']
    try:
        db = client[DB_NAME]
        now = datetime.utcnow()

        task = flow['task']
        account = flow['account']
        task_outbox = flow['task_outbox']
        account_outbox = flow['account_outbox']

        with client.start_session() as session:
            with session.start_transaction():
                # Insert task
                db.tasks.insert_one({
                    '_id': task_id,
                    'seq': seq,
                    'run_id': run_id,
                    'write_ts_ms': write_ts_ms,
                    'cin': task['cin'],
                    'type': task['type'],
                    'status': task['status'],
                    'request_payload': task['request_payload'],
                    'created_date': now,
                    'modified_date': now,
                }, session=session)

                # Insert task outbox
                db.outbox.insert_one({
                    '_id': task_outbox['outbox_id'],
                    'aggregate_type': task_outbox['aggregate_type'],
                    'aggregate_id': task_outbox['aggregate_id'],
                    'event_type': task_outbox['event_type'],
                    'payload': task_outbox['payload'],
                    'status': 'PENDING',
                    'created_date': now,
                }, session=session)

                # Insert account
                db.modular_accounts.insert_one({
                    '_id': account['account_id'],
                    'cin': account['cin'],
                    'sort_code': account['sort_code'],
                    'account_number': account['account_number'],
                    'name': account['name'],
                    'currency': account['currency'],
                    'status': account['status'],
                    'brand': account['brand'],
                    'initiated_by': account['initiated_by'],
                    'created_date': now,
                    'modified_date': now,
                }, session=session)

                # Insert account outbox
                db.outbox.insert_one({
                    '_id': account_outbox['outbox_id'],
                    'aggregate_type': account_outbox['aggregate_type'],
                    'aggregate_id': account_outbox['aggregate_id'],
                    'event_type': account_outbox['event_type'],
                    'payload': account_outbox['payload'],
                    'status': 'PENDING',
                    'created_date': now,
                }, session=session)

        completed_ms = int(time.time() * 1000)  # Capture AFTER commit
        duration_ms = (time.perf_counter() - t_start) * 1000
        stats.add(duration_ms, outbox_ids, started_ms, completed_ms)

        # Write insert_log row
        if log_writer is not None:
            log_writer.writerow([
                seq, run_id, task_id,
                f"{write_ts_ms:.3f}",
                f"{duration_ms:.3f}",
                '',
            ])

    except Exception as e:
        stats.add_error(outbox_ids, reason=e)
        logger.debug(f"Insert error: {e}")


def main():
    load_env()
    parser = argparse.ArgumentParser(description="MongoDB Golden Source Producer")
    parser.add_argument('--golden', type=str, required=True, help='Path to golden_source.json')
    parser.add_argument('--threads', type=int, default=10, help='Number of parallel threads')
    parser.add_argument('--run-id', type=str, default=None,
                        help='Unique run identifier (default: auto-generated epoch ms)')
    parser.add_argument('--log', type=str, default='insert_log.csv',
                        help='Path to write insert timing CSV (default: insert_log.csv)')

    args = parser.parse_args()

    run_id = args.run_id or str(int(time.time() * 1000))

    with open(args.golden, 'r') as f:
        data = json.load(f)

    flows = data['flows']
    total_flows = len(flows)
    del data  # Free memory - we only need the flows list
    logger.info(f"Loaded {total_flows} flows from {args.golden}")
    logger.info(f"Inserting into MongoDB with {args.threads} threads (run_id={run_id})...")

    # Open insert log CSV
    log_file = open(args.log, 'w', newline='')
    log_writer = csv.writer(log_file)
    log_writer.writerow(['seq', 'run_id', 'task_id', 'write_ts_ms', 'insert_ms', 'attempts'])

    # Create a single shared MongoClient (thread-safe with internal connection pool)
    client = MongoClient(MONGO_URI)

    stats = Stats()
    start_time = time.time()
    log_lock = threading.Lock()

    class ThreadSafeWriter:
        def writerow(self, row):
            with log_lock:
                log_writer.writerow(row)
                log_file.flush()

    safe_writer = ThreadSafeWriter()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {}
        for seq_idx, flow in enumerate(flows, start=1):
            future = executor.submit(insert_flow, flow, stats, client, seq_idx, run_id, safe_writer)
            futures[future] = seq_idx

        completed = 0
        for f in as_completed(futures):
            completed += 1
            if completed % 200 == 0:
                elapsed = time.time() - start_time
                logger.info(f"  Progress: {completed}/{total_flows} ({elapsed:.1f}s)")
            f.result()

    log_file.close()
    client.close()

    elapsed = time.time() - start_time
    throughput = stats.inserted / elapsed if elapsed > 0 else 0

    latencies = sorted(stats.latencies_ms) if stats.latencies_ms else [0]
    avg = sum(latencies) / len(latencies)
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print("\n" + "=" * 60)
    print("  MONGODB GOLDEN SOURCE PRODUCER RESULTS")
    print("=" * 60)
    print(f" Flows inserted:   {stats.inserted}")
    print(f" Outbox records:   {stats.inserted * 2}")
    print(f" Errors:           {stats.errors}")
    print(f" Duration:         {elapsed:.2f}s")
    print(f" Throughput:       {throughput:.1f} flows/s ({throughput * 2:.1f} outbox records/s)")
    print(f" Latency (per flow):")
    print(f"   Avg: {avg:.1f} ms")
    print(f"   P50: {p50:.1f} ms")
    print(f"   P95: {p95:.1f} ms")
    print(f"   P99: {p99:.1f} ms")
    print(f"   Min: {latencies[0]:.1f} ms")
    print(f"   Max: {latencies[-1]:.1f} ms")
    print("=" * 60 + "\n")

    # Write timestamps file
    ts_file = 'mongo_timestamps.json'
    with open(ts_file, 'w') as f:
        json.dump(stats.timestamps, f)
    print(f" Timestamps written to {ts_file} ({len(stats.timestamps)} records)")

    # Write failed IDs file
    if stats.failed_ids:
        failed_file = 'mongo_failed.json'
        with open(failed_file, 'w') as f:
            json.dump(stats.failed_ids, f)
        print(f"  Failed IDs written to {failed_file} ({len(stats.failed_ids)} records)")


if __name__ == '__main__':
    main()