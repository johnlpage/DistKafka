#!/usr/bin/env python3
"""
Insert a document into bank.payments with a local machine timestamp and hostname.
Usage:
  python3 producer.py

Requires:
  pymongo
  kafka-python (not used here, but installed as a dependency)

Environment variables (from .env):
  MONGO_URI - MongoDB connection string
  MONGO_DB_NAME - database name (default: bank)
  MONGO_COLLECTION - collection name (default: payments)
"""

import os
import sys
import json
import socket
import time
from datetime import datetime, timezone

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
                    # in bash; strip matching quotes here since this parser
                    # isn't shell-aware.
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    os.environ.setdefault(key, value)


def insert_with_retry(collection, doc, timeout_s=5.0, max_retries=None):
    """Insert `doc` (which must already have a fixed _id) with
    writeConcern={w:"majority"}, retrying if the attempt doesn't
    complete within `timeout_s` seconds.

    Two different, complementary timeouts are in play here, covering
    two different failure modes:

    1. writeConcern's `wtimeout` (set by the caller on `collection` via
       `.with_options(write_concern=WriteConcern(w="majority",
       wtimeout=...))`) - a SERVER-side bound: once the primary has
       actually received and executed the command, this is how long IT
       waits for replica acks before giving up and returning a clean
       WriteConcernError. This is the common, "healthy but replicating
       slowly" case, and firing this gives the nicest, fastest,
       most-informative failure.

    2. PyMongo's CSOT `pymongo.timeout()` (used below) - a CLIENT-side
       bound that fires regardless of what's happening server-side or
       even at the socket level. This is what actually saves us in the
       failure mode observed during DistKafka's regional-failover
       testing: mongos's lightweight replica-set monitor correctly
       detected the newly-elected primary as healthy within seconds,
       but its SEPARATE execution-pool connection to that same primary
       was wedged (killed mid-flight by the simulated outage, no clean
       TCP FIN/RST) - a write sent over it just hung with NO response
       at all, so `wtimeout` never even started its clock, because the
       primary never got the command in the first place. A plain
       client-side socket read in that situation blocks until the OS's
       own TCP retransmission logic eventually gives up (tens of
       minutes, and not configurable from here) - confirmed live:
       pointing PyMongo at a deliberately blackholed address,
       `pymongo.timeout(5)` reliably raised after exactly 5.00s
       regardless, where no server-side wtimeout would have fired at
       all.

    Both are set to the same `timeout_s` here for simplicity - whichever
    fires first (usually wtimeout, when the primary is reachable at
    all) triggers the same retry path below.

    Retrying with the SAME _id (rather than a fresh one per attempt)
    is deliberate: if an earlier attempt's write actually did make it
    to the primary but we simply timed out waiting to hear back, the
    retry will hit a DuplicateKeyError instead of silently inserting a
    second copy of the same logical document - at which point we know
    the original attempt succeeded and stop, rather than treating it
    as a failure.

    Returns (result, attempts, total_elapsed_ms).
    """
    attempt = 0
    t_start = time.perf_counter()

    while True:
        attempt += 1
        t0 = time.perf_counter()
        try:
            with pymongo.timeout(timeout_s):
                result = collection.insert_one(doc)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            total_ms = (time.perf_counter() - t_start) * 1000.0
            print(
                f"[attempt {attempt}] inserted after {elapsed_ms:.0f}ms "
                f"(total {total_ms:.0f}ms)",
                file=sys.stderr,
            )
            return result, attempt, total_ms

        except DuplicateKeyError:
            # A previous attempt's write actually landed (we just
            # didn't hear back from it in time) - nothing more to do.
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            total_ms = (time.perf_counter() - t_start) * 1000.0
            print(
                f"[attempt {attempt}] duplicate key after {elapsed_ms:.0f}ms - "
                f"a prior attempt already succeeded, stopping (total {total_ms:.0f}ms)",
                file=sys.stderr,
            )
            return None, attempt, total_ms

        except PyMongoError as exc:
            # Covers CSOT deadline expiry (the common case here) as
            # well as any other retryable-ish PyMongo error - either
            # way, we don't yet know if the write landed, so retry
            # (with the same _id) rather than give up or duplicate.
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            total_ms = (time.perf_counter() - t_start) * 1000.0
            print(
                f"[attempt {attempt}] did not complete within {timeout_s}s "
                f"({type(exc).__name__}: {exc}) - retrying (total {total_ms:.0f}ms)",
                file=sys.stderr,
            )
            if max_retries is not None and attempt >= max_retries:
                raise
            # No sleep/backoff by design: the CSOT deadline itself
            # already paces retries - a wedged mongos->primary
            # connection pool won't clear any faster for us waiting
            # longer between attempts, and we want to notice the
            # instant it does clear.
            continue


def main():
    load_env()

    mongo_uri = os.environ.get("MONGO_URI")
    db_name = os.environ.get("MONGO_DB_NAME", "bank")
    collection_name = os.environ.get("MONGO_COLLECTION", "payments")
    timeout_s = float(os.environ.get("MONGO_WRITE_TIMEOUT_S", "5"))
    max_retries = os.environ.get("MONGO_WRITE_MAX_RETRIES")
    max_retries = int(max_retries) if max_retries else None
    hostname = socket.gethostname()
    timestamp = datetime.now(timezone.utc)

    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable not set")
        print("Create a .env file or export MONGO_URI")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name].with_options(
        write_concern=WriteConcern(w="majority", wtimeout=int(timeout_s * 1000))
    )

    doc = {
        "_id": ObjectId(),
        "host_created": hostname,
        "time_created": timestamp.isoformat(),
        "message": f"Test document from {hostname} at {timestamp.isoformat()}",
        "source": "producer.py",
    }

    result, attempts, total_ms = insert_with_retry(
        collection, doc, timeout_s=timeout_s, max_retries=max_retries
    )
    print(json.dumps({
        "status": "inserted",
        "id": str(doc["_id"]),
        "attempts": attempts,
        "total_ms": round(total_ms, 1),
        "host_created": hostname,
        "time_created": timestamp.isoformat(),
    }))


if __name__ == "__main__":
    main()