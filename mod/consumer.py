"""
Outbox Consumer with Processing Lock & Metrics Logging

Consumes CDC events from Kafka, acquires a DB-backed processing lock per event
(cross-region idempotency), processes the event, and logs key metrics to a
separate CSV file for post-hoc latency analysis.

Metrics logged per event:
  - outbox_id, aggregate_type, event_type
  - kafka_offset, kafka_partition
  - created_date (from outbox record in CDC payload)
  - source_ts_ms (Debezium connector detection time)
  - kafka_timestamp (broker append time)
  - lock_acquire_ms (time to obtain processing lock)
  - lock_outcome (acquired / already_processed / failed)
  - consumer_region (configurable)

Usage:
  python3 consumer.py --topic mod_accounts.public.outbox --region region-a
  python3 consumer.py --topic mongo_accounts.modular-accounts.outbox --from-beginning --capture mongo_kafka_timestamps.json --expected-count 200000
"""

import argparse
import csv
import json
import os
import sys
import time
import logging
import uuid
import socket
from collections import Counter
from datetime import datetime

try:
    from kafka import KafkaConsumer
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip3 install kafka-python-ng")
    sys.exit(1)

try:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
except ImportError:
    pass

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    try:
        import ysql as psycopg2
        from ysql.extras import RealDictCursor
    except ImportError:
        psycopg2 = None

try:
    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError
except ImportError:
    MongoClient = None

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
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
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "mod_accounts")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')

YB_DB_CONFIG = {
    'host': os.environ.get("YB_HOST", 'modular-accounts-yugabyte.dev.dcb-data-dev.shared.banksvcs.net'),
    'port': int(os.environ.get("YB_PORT", '5433')),
    'dbname': os.environ.get("YB_DBNAME", 'mod_accounts'),
    'user': os.environ.get("YB_USER", 'modaccounts'),
    'password': os.environ.get("YB_PASSWORD", 'modularaccountstemppassword'),
    'sslmode': os.environ.get("YB_SSLMODE", 'verify-ca'),
    'sslrootcert': os.environ.get("YB_SSLROOTCERT", '/tmp/yb-ca-root-cert.crt'),
    'connect_timeout': int(os.environ.get("YB_CONNECT_TIMEOUT", '10')),
}

# Consumer identity
CONSUMER_ID = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

# -----------------------------------------------------------------------------
# Metrics Logger - CSV file for post-analysis
# -----------------------------------------------------------------------------
METRICS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'metrics')
os.makedirs(METRICS_DIR, exist_ok=True)

class MetricsLogger:
    """Thread-safe CSV metrics writer for consumer events."""

    COLUMNS = [
        'timestamp', 'outbox_id', 'aggregate_type', 'event_type',
        'kafka_offset', 'kafka_partition', 'kafka_timestamp_ms',
        'created_date_ms', 'source_ts_ms', 'payload_ts_ms',
        'cdc_propagation_ms', 'kafka_publish_ms', 'e2e_latency_ms',
        'lock_acquire_ms', 'lock_outcome', 'consumer_region', 'consumer_id',
        'seq', 'run_id', 'write_ts_ms', 'receipt_ts_ms',
    ]

    def __init__(self, filename):
        self.filepath = os.path.join(METRICS_DIR, filename)
        self.file = open(self.filepath, 'a', newline='')
        self.writer = csv.writer(self.file)
        if os.path.getsize(self.filepath) == 0:
            self.writer.writerow(self.COLUMNS)

    def log(self, **kwargs):
        row = [kwargs.get(col, '') for col in self.COLUMNS]
        self.writer.writerow(row)
        self.file.flush()

    def close(self):
        self.file.close()

# -----------------------------------------------------------------------------
# Processing Lock - YugabyteDB implementation
# -----------------------------------------------------------------------------
class YBProcessingLock:
    """
    Distributed processing lock using YugabyteDB.
    Uses INSERT with conflict detection for atomic lock acquisition.
    """

    def __init__(self, db_config, consumer_id, region):
        self.db_config = db_config
        self.consumer_id = consumer_id
        self.region = region
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = True

    def acquire(self, event_id):
        """
        Attempt to acquire processing lock for event_id.
        Returns (success: bool, already_processed: bool, duration_ms: float)
        """
        t_start = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO processing_locks (event_id, locked_by, locked_at, region)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (event_id, self.consumer_id, self.region))
                # rowcount == 1 means we got the lock; 0 means someone else has it
                acquired = cur.rowcount == 1
            t_end = time.perf_counter()
            duration_ms = (t_end - t_start) * 1000
            return acquired, not acquired, duration_ms
        except Exception as e:
            t_end = time.perf_counter()
            duration_ms = (t_end - t_start) * 1000
            logger.warning(f"Lock acquire error for {event_id}: {e}")
            # Reconnect on failure
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            return False, False, duration_ms

    def mark_processed(self, event_id):
        """Mark event as fully processed."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE processing_locks
                    SET processed_at = CURRENT_TIMESTAMP
                    WHERE event_id = %s AND locked_by = %s
                """, (event_id, self.consumer_id))
        except Exception as e:
            logger.warning(f"Mark processed error for {event_id}: {e}")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

# -----------------------------------------------------------------------------
# Processing Lock - MongoDB implementation
# -----------------------------------------------------------------------------
class MongoProcessingLock:
    """
    Distributed processing lock using MongoDB.
    Uses insert with duplicate key detection for atomic lock acquisition.
    """

    def __init__(self, mongo_uri, db_name, consumer_id, region):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db['processing_locks']
        self.consumer_id = consumer_id
        self.region = region
        # Ensure unique index on event_id
        self.collection.create_index("event_id", unique=True, sparse=True)

    def acquire(self, event_id):
        """
        Attempt to acquire processing lock for event_id.
        Returns (success: bool, already_processed: bool, duration_ms: float)
        """
        t_start = time.perf_counter()
        try:
            self.collection.insert_one({
                "_id": event_id,
                "event_id": event_id,
                "locked_by": self.consumer_id,
                "locked_at": datetime.utcnow(),
                "processed_at": None,
                "region": self.region,
            })
            t_end = time.perf_counter()
            return True, False, (t_end - t_start) * 1000
        except MongoDuplicateKeyError:
            t_end = time.perf_counter()
            return False, True, (t_end - t_start) * 1000
        except Exception as e:
            t_end = time.perf_counter()
            logger.warning(f"Lock acquire error for {event_id}: {e}")
            return False, False, (t_end - t_start) * 1000

    def mark_processed(self, event_id):
        """Mark event as fully processed."""
        try:
            self.collection.update_one(
                {"_id": event_id},
                {"$set": {"processed_at": datetime.utcnow()}}
            )
        except Exception as e:
            logger.warning(f"Mark processed error for {event_id}: {e}")

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass

# -----------------------------------------------------------------------------
# Debezium message parsing
# -----------------------------------------------------------------------------
def parse_debezium_message(raw_value):
    """
    Parse a CDC message (Debezium OR MongoDB Kafka Source Connector format)
    and extract:
      - outbox_id, aggregate_type, event_type, created_date
      - source_ts_ms (connector detection time)
      - payload-level ts_ms (connector emit time)
      - seq, run_id, write_ts_ms (from producer-embedded fields)
    Returns dict with extracted fields or None on parse failure.
    """
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode('utf-8')
        msg = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    # Handle double-encoded JSON (string inside string)
    for _ in range(3):
        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, ValueError):
                break
        else:
            break

    if not isinstance(msg, dict):
        return None

    # Navigate envelope — try Debezium format first, then MongoDB Source Connector
    payload = msg.get('payload', msg)
    after = payload.get('after') if isinstance(payload, dict) else None
    if after is None:
        after = payload.get('before') if isinstance(payload, dict) else None

    # MongoDB Kafka Source Connector: document body is in fullDocument
    if not isinstance(after, dict) and isinstance(payload, dict):
        after = payload.get('fullDocument')

    # Last resort: treat the whole message as the document
    if not isinstance(after, dict):
        after = msg

    # Skip non-insert events — they have no fullDocument.
    # MongoDB Source Connector uses operationType; Debezium uses op ('c' = create).
    if isinstance(payload, dict):
        op = payload.get('operationType') or payload.get('op')
        if op is not None and op not in ('insert', 'create', 'c', 'INSERT'):
            return None

    # Extract source.ts_ms (Debezium) or wallTime (MongoDB Source Connector)
    source = payload.get('source', {}) if isinstance(payload, dict) else {}
    source_ts_ms = source.get('ts_ms')
    if source_ts_ms is None and isinstance(payload, dict):
        wall_time = payload.get('wallTime')
        if isinstance(wall_time, dict):
            source_ts_ms = wall_time.get('$date')
        elif isinstance(wall_time, (int, float)):
            source_ts_ms = wall_time

    # Extract top-level payload ts_ms (Debezium emit time).
    # Do NOT fall back to clusterTime — that's a BSON Timestamp dict {"$timestamp": {"t": ..., "i": ...}},
    # not epoch ms, and can't be compared with kafka_ts.
    payload_ts_ms = payload.get('ts_ms') if isinstance(payload, dict) else None

    # Extract outbox fields — support both raw values and BSON-wrapped {"$date": ..., "$oid": ...}
    outbox_id = _unwrap(after.get('_id'))
    aggregate_type = _unwrap(after.get('aggregate_type'))
    event_type = _unwrap(after.get('event_type'))
    created_date_raw = after.get('created_date')

    # Parse created_date to epoch ms
    created_date_ms = None
    if created_date_raw:
        created_date_ms = _parse_timestamp_to_epoch_ms(created_date_raw)

    # Extract write-side fields for latency join
    seq = _unwrap(after.get('seq')) if isinstance(after, dict) else None
    run_id = _unwrap(after.get('run_id')) if isinstance(after, dict) else None
    write_ts_ms_raw = after.get('write_ts_ms') if isinstance(after, dict) else None
    if isinstance(write_ts_ms_raw, dict):
        for key in ('$numberDouble', '$numberLong', '$numberInt'):
            if key in write_ts_ms_raw:
                try:
                    write_ts_ms_raw = float(write_ts_ms_raw[key])
                except (ValueError, TypeError):
                    write_ts_ms_raw = None
                break
        else:
            write_ts_ms_raw = None
    if isinstance(write_ts_ms_raw, (int, float)):
        write_ts_ms = write_ts_ms_raw
    else:
        write_ts_ms = None

    return {
        'outbox_id': outbox_id,
        'aggregate_type': aggregate_type,
        'event_type': event_type,
        'created_date_ms': created_date_ms,
        'source_ts_ms': source_ts_ms,
        'payload_ts_ms': payload_ts_ms,
        'seq': seq,
        'run_id': run_id,
        'write_ts_ms': write_ts_ms,
    }

def _unwrap(value):
    """Recursively unwrap Debezium struct wrappers to a plain string.
    Also handles MongoDB Extended JSON wrappers like {"$oid": "..."}."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ('value', 'string', 'uuid', 'payload',
                     '$oid', '$numberLong', '$numberInt', '$numberDouble'):
            if key in value:
                return _unwrap(value[key])
        return json.dumps(value)
    return str(value)

def _parse_timestamp_to_epoch_ms(ts):
    """Parse various timestamp formats to epoch milliseconds.
    Handles int, float, string ISO-8601, and MongoDB Extended JSON {"$date": ...}."""
    if isinstance(ts, dict):
        # MongoDB Extended JSON: {"$date": "2024-01-15T10:30:00.123Z"} or {"$date": 1705312200123}
        ts = ts.get('$date', ts)
    if isinstance(ts, (int, float)):
        # Already epoch - could be seconds, ms, or microseconds
        if ts > 1e15:  # microseconds
            return ts / 1000
        elif ts > 1e12:  # milliseconds
            return ts
        else:  # seconds
            return ts * 1000
    if isinstance(ts, str):
        # Try ISO format
        for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
            try:
                dt = datetime.strptime(ts, fmt)
                return dt.timestamp() * 1000
            except ValueError:
                continue
    return None

# -----------------------------------------------------------------------------
# Topic management
# -----------------------------------------------------------------------------
def clear_topic(bootstrap_servers, topic):
    """Delete and recreate a kafka topic."""
    logger.info(f"Clearing topic: {topic}")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.delete_topics([topic])
        time.sleep(3)
    except Exception:
        pass
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except Exception:
        pass
    admin.close()

# -----------------------------------------------------------------------------
# Main consumer loop
# -----------------------------------------------------------------------------
def consume_and_process(topic, bootstrap_servers, timeout_sec, from_beginning,
                        expected_count, max_wait, region, db_type, report_interval, skip_lock=False):
    """
    Consume events, acquire processing lock, log metrics.
    """
    logger.info(f"Consumer starting: topic={topic}, region={region}, db_type={db_type}")
    logger.info(f"Consumer ID: {CONSUMER_ID}")

    # Metrics logger
    metrics_filename = f"consumer_metrics_{db_type}_{region}.csv"
    metrics = MetricsLogger(metrics_filename)

    # Processing lock
    if db_type == 'yb':
        lock = YBProcessingLock(YB_DB_CONFIG, CONSUMER_ID, region)
    else:
        lock = MongoProcessingLock(MONGO_URI, MONGO_DB_NAME, CONSUMER_ID, region)

    # Kafka consumer
    auto_offset = 'earliest' if from_beginning else 'latest'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=timeout_sec * 1000,
        value_deserializer=lambda m: m,
    )

    # Counters
    total_messages = 0
    processed_count = 0
    already_processed_count = 0
    lock_failed_count = 0
    parse_errors = 0
    event_type_counter = Counter()

    start_time = time.time()
    last_report = start_time

    logger.info("Consuming messages...")

    try:
        while True:
            if expected_count and total_messages >= expected_count:
                logger.info(f"Reached expected count ({expected_count}). Stopping.")
                break
            if max_wait and (time.time() - start_time) >= max_wait:
                logger.info(f"Max wait ({max_wait}s) reached. Stopping.")
                break

            records = consumer.poll(timeout_ms=2000)

            for tp, messages in records.items():
                for message in messages:
                    total_messages += 1

                    # Parse Debezium message
                    parsed = parse_debezium_message(message.value)
                    if parsed is None or parsed['outbox_id'] is None:
                        parse_errors += 1
                        continue

                    outbox_id = parsed['outbox_id']
                    kafka_ts = message.timestamp  # broker append time (ms)
                    receipt_ts_ms = time.time() * 1000  # consumer receipt time

                    # Compute latencies
                    cdc_propagation_ms = ''
                    kafka_publish_ms = ''
                    e2e_latency_ms = ''

                    if isinstance(parsed.get('source_ts_ms'), (int, float)) and isinstance(parsed.get('created_date_ms'), (int, float)):
                        cdc_propagation_ms = parsed['source_ts_ms'] - parsed['created_date_ms']

                    if isinstance(kafka_ts, (int, float)) and isinstance(parsed.get('payload_ts_ms'), (int, float)):
                        kafka_publish_ms = kafka_ts - parsed['payload_ts_ms']

                    if isinstance(kafka_ts, (int, float)) and isinstance(parsed.get('created_date_ms'), (int, float)):
                        e2e_latency_ms = kafka_ts - parsed['created_date_ms']

                    # Acquire processing lock (skip if --skip-lock is set)
                    if skip_lock:
                        acquired = True
                        already_done = False
                        lock_ms = 0
                        lock_outcome = 'skipped'
                        processed_count += 1
                    else:
                        acquired, already_done, lock_ms = lock.acquire(outbox_id)

                    if skip_lock or acquired:
                        lock_outcome = 'acquired' if not skip_lock else 'skipped'
                        processed_count += 1
                        if not skip_lock:
                            lock.mark_processed(outbox_id)
                    elif already_done:
                        lock_outcome = 'already_processed'
                        already_processed_count += 1
                    else:
                        lock_outcome = 'failed'
                        lock_failed_count += 1

                    event_type_counter[parsed['event_type'] or 'UNKNOWN'] += 1

                    # Log metrics
                    metrics.log(
                        timestamp=datetime.utcnow().isoformat(timespec='milliseconds'),
                        outbox_id=outbox_id,
                        aggregate_type=parsed['aggregate_type'] or '',
                        event_type=parsed['event_type'] or '',
                        kafka_offset=message.offset,
                        kafka_partition=message.partition,
                        kafka_timestamp_ms=kafka_ts or '',
                        created_date_ms=parsed['created_date_ms'] or '',
                        source_ts_ms=parsed['source_ts_ms'] or '',
                        payload_ts_ms=parsed['payload_ts_ms'] or '',
                        cdc_propagation_ms=f"{cdc_propagation_ms:.1f}" if isinstance(cdc_propagation_ms, (int, float)) else '',
                        kafka_publish_ms=f"{kafka_publish_ms:.1f}" if isinstance(kafka_publish_ms, (int, float)) else '',
                        e2e_latency_ms=f"{e2e_latency_ms:.1f}" if isinstance(e2e_latency_ms, (int, float)) else '',
                        lock_acquire_ms=f"{lock_ms:.3f}",
                        lock_outcome=lock_outcome,
                        consumer_region=region,
                        consumer_id=CONSUMER_ID,
                        seq=parsed.get('seq') or '',
                        run_id=parsed.get('run_id') or '',
                        write_ts_ms=parsed.get('write_ts_ms') or '',
                        receipt_ts_ms=f"{receipt_ts_ms:.3f}",
                    )

            if expected_count and total_messages >= expected_count:
                break

            # Progress report
            now = time.time()
            if now - last_report >= report_interval:
                elapsed = now - start_time
                logger.info(
                    f" Progress: {total_messages} consumed, {processed_count} processed, "
                    f"{already_processed_count} skipped (already done), "
                    f"{lock_failed_count} lock failures ({elapsed:.1f}s)"
                )
                last_report = now

    except KeyboardInterrupt:
        logger.info("Interrupted. Generating report...")
    finally:
        consumer.close()
        lock.close()
        metrics.close()

    elapsed = time.time() - start_time

    # Final report
    print("\n" + "=" * 70)
    print(" CONSUMER PROCESSING REPORT")
    print("=" * 70)
    print(f"\n Consumer ID:       {CONSUMER_ID}")
    print(f" Region:            {region}")
    print(f" DB Type:           {db_type}")
    print(f" Topic:             {topic}")
    print(f" Duration:          {elapsed:.1f}s")
    print(f"\n CONSUMPTION:")
    print(f"   Total messages:  {total_messages}")
    print(f"   Parse errors:    {parse_errors}")
    print(f"\n PROCESSING LOCK:")
    print(f"   Acquired & processed: {processed_count}")
    print(f"   Already processed:    {already_processed_count}")
    print(f"   Lock failures:        {lock_failed_count}")
    print(f"\n EVENT TYPES:")
    for evt, count in event_type_counter.most_common():
        print(f"   {evt:30s} {count}")
    print(f"\n METRICS FILE: {metrics.filepath}")
    print("=" * 70 + "\n")

# -----------------------------------------------------------------------------
# Fast ID extraction for capture mode (no full Debezium parse needed)
# -----------------------------------------------------------------------------
def _extract_id_fast(raw_value):
    """
    Lightweight _id extraction - skip full Debezium parsing.
    Just decode, unwrap strings, and grab _id.
    """
    if isinstance(raw_value, bytes):
        try:
            raw_value = raw_value.decode('utf-8')
        except UnicodeDecodeError:
            return None

    # Unwrap nested JSON strings (up to 3 levels)
    doc = raw_value
    for _ in range(3):
        if isinstance(doc, str):
            try:
                doc = json.loads(doc)
            except (json.JSONDecodeError, ValueError):
                return None
        else:
            break

    if not isinstance(doc, dict):
        return None

    # Check for Debezium envelope
    if 'payload' in doc:
        payload = doc['payload']
        if isinstance(payload, dict):
            after = payload.get('after')
            if isinstance(after, dict):
                doc = after

    oid = doc.get('_id')
    if oid is None:
        return None
    if isinstance(oid, dict):
        oid = oid.get('value') or oid.get('$oid') or str(oid)
    return str(oid) if oid else None

# -----------------------------------------------------------------------------
# Capture mode - just consume and write kafka timestamps to file
# -----------------------------------------------------------------------------
def capture_kafka_timestamps(topic, bootstrap_servers, output_path, expected_count, max_wait, from_beginning):
    """
    Consume from Kafka and write outbox_id -> kafka_broker_timestamp_ms to a JSON file.
    No lock processing, no DB interaction. Just captures timestamps.
    """
    logger.info(f"Capture mode: topic={topic}, output={output_path}")
    logger.info(f" Expected count: {expected_count} (no timeout, waits until all received)")

    auto_offset = 'earliest' if from_beginning else 'latest'
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: m,
        fetch_max_bytes=5 * 1024 * 1024,         # 5MB per fetch (default 1MB)
        max_partition_fetch_bytes=2 * 1024 * 1024, # 2MB per partition (default 1MB)
    )

    # Wait for topic
    from kafka import TopicPartition
    logger.info(f" Waiting for topic '{topic}'...")
    topic_wait_start = time.time()
    partitions = None
    while (time.time() - topic_wait_start) < 120:
        consumer.topics()
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            break
        time.sleep(3)

    if not partitions:
        logger.error(f" Topic '{topic}' not found!")
        consumer.close()
        sys.exit(1)

    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)

    # Stream timestamps directly to file instead of holding in memory
    captured_count = 0
    parse_errors = 0
    start = time.time()
    last_message_time = None
    idle_timeout = 30

    output_file = open(output_path, 'w')
    output_file.write('{')
    first_entry = True

    while True:
        records = consumer.poll(timeout_ms=3000, max_records=5000)
        if not records:
            if captured_count >= expected_count:
                logger.info(f" Reached expected count ({expected_count}). Stopping.")
                break
            if last_message_time and (time.time() - last_message_time) > idle_timeout:
                logger.info(f" No new messages for {idle_timeout}s. Stopping with {captured_count} records.")
                break
            continue

        last_message_time = time.time()
        for tp, messages in records.items():
            for msg in messages:
                oid = _extract_id_fast(msg.value)
                if oid:
                    if not first_entry:
                        output_file.write(',')
                    output_file.write(f'\n  {json.dumps(oid)}: {msg.timestamp}')
                    first_entry = False
                    captured_count += 1
                else:
                    parse_errors += 1

        if captured_count >= expected_count:
            logger.info(f" Reached expected count ({expected_count}). Stopping.")
            break

    output_file.write('\n}\n')
    output_file.close()

    elapsed = time.time() - start
    consumer.close()

    print(f"\nKafka timestamp capture complete:")
    print(f"  Unique IDs captured: {captured_count}")
    print(f"  Parse errors:       {parse_errors}")
    print(f"  Duration:           {elapsed:.1f}s")
    print(f"  Output:             {output_path}")

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Outbox Consumer with Processing Lock & Metrics"
    )
    parser.add_argument('--topic', type=str, default=os.environ.get('KAFKA_TOPIC', 'mongo_accounts.modular-accounts.outbox'),
                        help='Kafka topic (default: from KAFKA_TOPIC env or mongo_accounts.modular-accounts.outbox)')
    parser.add_argument('--bootstrap-servers', type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument('--timeout', type=int, default=5,
                        help='Poll timeout seconds per batch (default: 5)')
    parser.add_argument('--from-beginning', action='store_true')
    parser.add_argument('--expected-count', type=int, default=20000)
    parser.add_argument('--max-wait', type=int, default=120)
    parser.add_argument('--report-interval', type=int, default=30)
    parser.add_argument('--region', type=str, default='region-a',
                        help='Consumer region identifier (default: region-a)')
    parser.add_argument('--db-type', type=str, choices=['yb', 'mongo'], default='yb',
                        help='Database type for lock storage (default: yb)')
    parser.add_argument('--skip-lock', action='store_true',
                        help='Skip processing lock (faster, no idempotency)')
    parser.add_argument('--drop', action='store_true',
                        help='Clear the topic before consuming')
    parser.add_argument('--capture', type=str, default=None,
                        help='Capture mode: consume and write kafka timestamps to this file (no lock processing)')

    args = parser.parse_args()

    if args.drop:
        clear_topic(args.bootstrap_servers, args.topic)
        sys.exit(0)

    if args.capture:
        capture_kafka_timestamps(
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            output_path=args.capture,
            expected_count=args.expected_count,
            max_wait=args.max_wait,
            from_beginning=args.from_beginning,
        )
        sys.exit(0)

    consume_and_process(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        timeout_sec=args.timeout,
        from_beginning=args.from_beginning,
        expected_count=args.expected_count,
        max_wait=args.max_wait,
        region=args.region,
        db_type=args.db_type,
        report_interval=args.report_interval,
        skip_lock=args.skip_lock,
    )

if __name__ == '__main__':
    main()