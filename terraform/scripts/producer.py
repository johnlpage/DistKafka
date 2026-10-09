#!/usr/bin/env python3
"""
Insert a task, task-outbox, account, and account-outbox document (four
documents total) in a single server call, wrapped in a multi-document
transaction, retrying the whole transaction on failure. This mirrors the
four-document write shape used by outbox_producer.py, but as an actual
ACID transaction (session.with_transaction()) instead of a best-effort
ordered bulk write - so either all four documents land, or none do.

The task document carries the same host_created/time_created/message
fields the old single-document smoke test used, and lives in the
collection the Kafka Source Connector watches (MONGO_COLLECTION,
default "tasks") - so consumer.py's tally of "one row per producer run"
still holds.

Usage:
  python3 producer.py

Requires:
  pymongo
  kafka-python (not used here, but installed as a dependency)

Environment variables (from .env):
  MONGO_URI - MongoDB connection string
  MONGO_DB_NAME - database name (default: bank)
  MONGO_COLLECTION - task collection name, watched by Kafka (default: tasks)
  MONGO_ACCOUNT_COLLECTION - account collection name (default: modular_accounts)
  MONGO_OUTBOX_COLLECTION - outbox collection name (default: outbox)
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
    from pymongo.errors import ClientBulkWriteException, PyMongoError
    from bson import ObjectId
except ImportError:
    print("ERROR: pymongo not installed. Run: pip3 install pymongo")
    sys.exit(1)


DUPLICATE_KEY_CODE = 11000


def _is_duplicate_key_error(exc):
    """True if `exc` is a ClientBulkWriteException (the error type
    client.bulk_write() raises - see pymongo.errors) whose write errors
    include a duplicate-key (11000) error. client-level bulk_write()
    doesn't raise the older DuplicateKeyError for this - it always
    raises ClientBulkWriteException and reports individual operation
    failures in its `write_errors` list.
    """
    if not isinstance(exc, ClientBulkWriteException):
        return False
    return any(err.get("code") == DUPLICATE_KEY_CODE for err in (exc.write_errors or []))


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


def build_models(db_name, collection_name, account_collection_name,
                  outbox_collection_name, task_id, account_id,
                  task_outbox_id, account_outbox_id, hostname, timestamp):
    """Build the four InsertOne models for one task/account/outbox flow.
    Called fresh on every retry attempt (so documents reflect the
    attempt's own namespaces), but always with the SAME four _ids,
    so a retry after an uncertain commit can be detected as a
    duplicate rather than silently writing a second copy.
    """
    return [
        pymongo.InsertOne(
            {
                "_id": task_id,
                "host_created": hostname,
                "time_created": timestamp.isoformat(),
                "message": f"Test document from {hostname} at {timestamp.isoformat()}",
                "source": "producer.py",
            },
            namespace=f"{db_name}.{collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": task_outbox_id,
                "aggregate_type": "task",
                "aggregate_id": task_id,
                "event_type": "TASK_CREATED",
                "payload": {"task_id": task_id},
                "status": "PENDING",
                "created_date": timestamp,
            },
            namespace=f"{db_name}.{outbox_collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": account_id,
                "host_created": hostname,
                "time_created": timestamp.isoformat(),
                "name": "DistKafka transaction smoke test",
                "status": "ACTIVE",
            },
            namespace=f"{db_name}.{account_collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": account_outbox_id,
                "aggregate_type": "account",
                "aggregate_id": account_id,
                "event_type": "ACCOUNT_CREATED",
                "payload": {"account_id": account_id},
                "status": "PENDING",
                "created_date": timestamp,
            },
            namespace=f"{db_name}.{outbox_collection_name}",
        ),
    ]


def insert_with_retry(client, build_models_fn, timeout_s=5.0, max_retries=None):
    """Run the four-document insert as one transaction (one
    bulk_write() server call across all four namespaces, wrapped in
    session.with_transaction()), retrying the WHOLE transaction if the
    attempt doesn't complete within `timeout_s` seconds.

    Three different, complementary timeouts are in play here, covering
    three different failure modes:

    1. The transaction's own majority write concern - a SERVER-side
       bound: once the primary has actually received and executed the
       commit, this is how long IT waits for replica acks before giving
       up and returning a clean WriteConcernError. This is the common,
       "healthy but replicating slowly" case, and firing this gives the
       nicest, fastest, most-informative failure. `max_commit_time_ms`
       (passed to with_transaction() below, set to the same
       `timeout_s`) gives the COMMIT specifically an explicit maxTimeMS
       bound on the server side - the transactional equivalent of the
       old single-document code's `wtimeout`, which doesn't apply here
       since write concern for a transaction is only meaningful at
       commit time, not on the individual statements inside it.

    2. `connectTimeoutMS`/`socketTimeoutMS`/`serverSelectionTimeoutMS`
       (set on the MongoClient itself, in main() below, all equal to
       `timeout_s * 1000`) - a CLIENT-side, per-socket-operation hard
       cap that applies uniformly to EVERY network call this client
       makes (bulk_write's insert command, the commit, server
       selection, reconnect attempts), independent of CSOT. This is a
       deliberate belt-and-braces backstop: client-level bulk_write()
       (the API used here, added in PyMongo 4.9 for MongoDB 8.0+) is
       newer and far less battle-tested than the classic insert_one()
       path, and switching from one to the other observably increased
       how long it took to detect a dead connection during
       DistKafka's regional-failover testing - i.e. CSOT alone
       (below) was not reliably bounding BOTH the bulk_write and the
       commit to the same tight deadline. Setting explicit, low,
       classic socket timeouts directly on the client removes any
       dependence on CSOT correctly propagating its deadline through
       every code path inside session.with_transaction()'s retry/pin/
       commit machinery, and instead guarantees a hard stop at the
       socket layer no matter which operation is in flight.

    3. PyMongo's CSOT `pymongo.timeout()` (used below) - a CLIENT-side
       bound that fires regardless of what's happening server-side or
       even at the socket level. This is what actually saves us in the
       failure mode observed during DistKafka's regional-failover
       testing: mongos's lightweight replica-set monitor correctly
       detected the newly-elected primary as healthy within seconds,
       but its SEPARATE execution-pool connection to that same primary
       was wedged (killed mid-flight by the simulated outage, no clean
       TCP FIN/RST) - a write sent over it just hung with NO response
       at all. A plain client-side socket read in that situation blocks
       until the OS's own TCP retransmission logic eventually gives up
       (tens of minutes, and not configurable from here) - confirmed
       live: pointing PyMongo at a deliberately blackholed address,
       `pymongo.timeout(5)` reliably raised after exactly 5.00s
       regardless, where no server-side wtimeout would have fired at
       all. Kept as a second, independent layer alongside (2) above,
       since it also bounds the ENTIRE with_transaction() call
       (bulk_write + commit together, plus any internal retries
       pymongo's transaction machinery performs on its own) to the same
       total deadline, not just each individual socket operation.

    `session.with_transaction()` already retries internally on
    TransientTransactionError and UnknownTransactionCommitResult, but
    only within whatever deadline is in effect - so the outer
    `pymongo.timeout()` block here still bounds the whole thing, and
    the outer loop below re-tries the ENTIRE transaction (fresh
    session, fresh attempt) with the SAME four _ids if that deadline
    is hit: if an earlier attempt's commit actually succeeded but we
    just timed out waiting to hear back, the retry hits a
    DuplicateKeyError on the task document instead of silently
    inserting a second copy of the whole flow - at which point we know
    the original attempt succeeded and stop, rather than treating it as
    a failure.

    Returns (result, attempts, total_elapsed_ms).
    """
    attempt = 0
    t_start = time.perf_counter()

    while True:
        attempt += 1
        t0 = time.perf_counter()
        try:
            def run_transaction(session):
                # write_concern is NOT passed to bulk_write() here: once a
                # transaction has started, pymongo requires the write
                # concern to come from the transaction itself (passed to
                # with_transaction() below) - passing one on an individual
                # operation inside an active transaction raises
                # InvalidOperation.
                return client.bulk_write(
                    build_models_fn(),
                    ordered=True,
                    session=session,
                )

            with pymongo.timeout(timeout_s):
                with client.start_session() as session:
                    # start_session() itself makes no network call - the
                    # logical session only actually starts with the first
                    # command sent on it (bulk_write's insert, below).
                    result = session.with_transaction(
                        run_transaction,
                        write_concern=WriteConcern(w="majority"),
                        max_commit_time_ms=int(timeout_s * 1000),
                    )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            total_ms = (time.perf_counter() - t_start) * 1000.0
            print(
                f"[attempt {attempt}] transaction committed (4 documents) after "
                f"{elapsed_ms:.0f}ms (total {total_ms:.0f}ms)",
                file=sys.stderr,
            )
            return result, attempt, total_ms

        except PyMongoError as exc:
            if _is_duplicate_key_error(exc):
                # A previous attempt's transaction actually committed (we
                # just didn't hear back from it in time) - nothing more
                # to do. Since the whole flow is one transaction, seeing
                # the task document's _id again means all four documents
                # from that attempt are already in place.
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                total_ms = (time.perf_counter() - t_start) * 1000.0
                print(
                    f"[attempt {attempt}] duplicate key after {elapsed_ms:.0f}ms - "
                    f"a prior transaction already committed, stopping (total {total_ms:.0f}ms)",
                    file=sys.stderr,
                )
                return None, attempt, total_ms

            # Covers CSOT deadline expiry (the common case here) as
            # well as any other retryable-ish PyMongo error - either
            # way, we don't yet know if the transaction committed, so
            # retry the whole thing (with the same four _ids) rather
            # than give up or duplicate.
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            total_ms = (time.perf_counter() - t_start) * 1000.0
            print(
                f"[attempt {attempt}] transaction did not complete within {timeout_s}s "
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
    collection_name = os.environ.get("MONGO_COLLECTION", "tasks")
    account_collection_name = os.environ.get("MONGO_ACCOUNT_COLLECTION", "modular_accounts")
    outbox_collection_name = os.environ.get("MONGO_OUTBOX_COLLECTION", "outbox")
    timeout_s = float(os.environ.get("MONGO_WRITE_TIMEOUT_S", "5"))
    max_retries = os.environ.get("MONGO_WRITE_MAX_RETRIES")
    max_retries = int(max_retries) if max_retries else None
    hostname = socket.gethostname()
    timestamp = datetime.now(timezone.utc)

    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable not set")
        print("Create a .env file or export MONGO_URI")
        sys.exit(1)

    client = MongoClient(
        mongo_uri,
        # Explicit, LOW classic socket timeouts (independent of CSOT -
        # see insert_with_retry()'s docstring): a hard backstop on
        # every individual socket operation this client performs
        # (bulk_write's insert command, the transaction commit, server
        # selection, reconnects), so detecting a dead/wedged connection
        # can never silently take longer than timeout_s regardless of
        # which operation (bulk_write vs commit) happens to be in
        # flight when the connection dies.
        connectTimeoutMS=int(timeout_s * 1000),
        socketTimeoutMS=int(timeout_s * 1000),
        serverSelectionTimeoutMS=int(timeout_s * 1000),
    )

    task_id = ObjectId()
    account_id = ObjectId()
    task_outbox_id = ObjectId()
    account_outbox_id = ObjectId()

    def build_models_fn():
        return build_models(
            db_name, collection_name, account_collection_name,
            outbox_collection_name, task_id, account_id,
            task_outbox_id, account_outbox_id, hostname, timestamp,
        )

    result, attempts, total_ms = insert_with_retry(
        client, build_models_fn, timeout_s=timeout_s, max_retries=max_retries
    )
    print(json.dumps({
        "status": "inserted",
        "id": str(task_id),
        "attempts": attempts,
        "total_ms": round(total_ms, 1),
        "host_created": hostname,
        "time_created": timestamp.isoformat(),
    }))


if __name__ == "__main__":
    main()
