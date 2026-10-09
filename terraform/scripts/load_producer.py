#!/usr/bin/env python3
"""
Load-test producer: inserts N four-document flows into MongoDB one at a
time (still one transaction - one bulk_write() server call across four
namespaces, inside session.with_transaction() - per flow, just many of
them in flight concurrently via a thread pool), timing each individual
transaction, and logs the results to a CSV for later latency analysis
(see compute_load_stats.py).

Each flow writes four documents in a single ACID transaction: a task,
a task-outbox record, an account, and an account-outbox record - the
same four-document shape as outbox_producer.py, but committed
atomically instead of as a best-effort ordered bulk write. Only the
task document (in MONGO_COLLECTION, the collection the Kafka Source
Connector watches) is observed downstream, so the per-run document
count received by the consumer is unchanged: one task document per
transaction, same as the old one-document-per-insert_one() shape.

The task document embeds:
  seq          - sequence number (1..count), used to join with the
                 consumer's receipt log
  run_id       - unique identifier for this run, so the consumer can
                 ignore any stale/leftover messages from a previous run
  write_ts_ms  - wall-clock epoch-ms timestamp (time.time()) taken
                 immediately before the transaction is attempted.

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
  MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION, MONGO_ACCOUNT_COLLECTION,
  MONGO_OUTBOX_COLLECTION
"""

import os
import sys
import csv
import json
import time
import socket
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pymongo
    import pymongo.monitoring as monitoring
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


# ---------------------------------------------------------------------------
# Optional SDAM (Server Discovery And Monitoring) + command instrumentation,
# enabled via --sdam-log. Purely diagnostic: lets you watch EXACTLY what
# PyMongo's driver internals are doing (which server it's talking to, how
# long each network call takes, when the driver's own background health
# monitor notices a server is down, when its topology view changes) during
# a live failover drill - whether triggered via toggle-mongos-block.sh's
# network-level block, or Atlas's own built-in resilience/fault-injection
# testing feature - the goal being to see directly whether a ~30s stall
# is "the driver keeps re-selecting the same now-dead mongos for several
# retries before its monitor catches up", "the monitor itself takes ~30s
# to flag the dead server", or something else entirely, rather than
# guessing.
#
# Four SEPARATE listener classes are required here, each registered
# independently via pymongo.monitoring.register() - NOT combined via
# multiple inheritance. ServerListener and TopologyListener both define
# opened()/description_changed()/closed(), and CommandListener and
# ServerHeartbeatListener both define started()/succeeded()/failed(); if
# any two of these were combined into one class, Python's MRO would
# silently pick only ONE implementation and the other listener's callbacks
# would simply never fire - a real, easy-to-miss gotcha with this API.
#
# All four listener types funnel through one thread-safe _SdamWriter,
# since callbacks fire from whichever thread triggered them: heartbeat
# callbacks run on PyMongo's own internal per-server monitor threads,
# command callbacks run on whichever application thread issued the write
# (one of this script's worker threads) - concurrent, unsynchronized
# writes to the same file would otherwise interleave/corrupt lines.
class _SdamWriter:
    def __init__(self, path):
        self._f = open(path, "w", buffering=1)  # line-buffered
        self._lock = threading.Lock()
        self._closed = False

    def write(self, listener, event, **fields):
        row = {"ts_ms": round(time.time() * 1000.0, 3), "listener": listener, "event": event}
        row.update(fields)
        line = json.dumps(row, default=str)
        with self._lock:
            if self._closed:
                # client.close() cancels the monitor threads' in-flight
                # heartbeats, but that cancellation itself gets reported
                # as a "failed" heartbeat callback asynchronously, which
                # can still fire a moment AFTER we've already decided to
                # stop logging (see close() below) - harmless, just
                # drop it rather than erroring out on shutdown.
                return
            self._f.write(line + "\n")

    def close(self):
        with self._lock:
            self._closed = True
            self._f.close()


def _server_desc_fields(desc):
    """Pull out the handful of ServerDescription fields that matter for
    failover diagnosis: is this server currently considered reachable/
    writable, what type does the driver think it is, and - if it's
    marked unreachable - what error caused that."""
    return {
        "address": f"{desc.address[0]}:{desc.address[1]}" if desc.address else None,
        "server_type": desc.server_type_name,
        "is_writable": desc.is_writable,
        "round_trip_time_ms": (
            round(desc.round_trip_time * 1000.0, 1) if desc.round_trip_time is not None else None
        ),
        "error": str(desc.error) if desc.error else None,
    }


class _SdamCommandListener(monitoring.CommandListener):
    """Logs actual commands (bulk_write's insert, commitTransaction).
    Successes are skipped by default (at load-test concurrency/volume
    they'd flood the log with no diagnostic value for failover analysis)
    unless --sdam-log-command-successes is passed.
    """

    def __init__(self, writer, log_successes):
        self._writer = writer
        self._log_successes = log_successes

    def started(self, event):
        pass  # not logged - see class docstring

    def succeeded(self, event):
        if not self._log_successes:
            return
        self._writer.write(
            "command", "succeeded",
            command_name=event.command_name,
            server=f"{event.connection_id[0]}:{event.connection_id[1]}",
            operation_id=event.operation_id,
            duration_ms=round(event.duration_micros / 1000.0, 1),
        )

    def failed(self, event):
        self._writer.write(
            "command", "failed",
            command_name=event.command_name,
            server=f"{event.connection_id[0]}:{event.connection_id[1]}",
            operation_id=event.operation_id,
            duration_ms=round(event.duration_micros / 1000.0, 1),
            error=str(event.failure),
        )


class _SdamHeartbeatListener(monitoring.ServerHeartbeatListener):
    """Logs the driver's OWN background health-check pings to each server
    in the topology - independent of any application traffic. This is the
    key signal for "did the monitor itself take ~30s to notice", separate
    from "did OUR writes take ~30s to stop hitting the dead server".
    """

    def __init__(self, writer):
        self._writer = writer

    def started(self, event):
        self._writer.write(
            "heartbeat", "started",
            server=f"{event.connection_id[0]}:{event.connection_id[1]}",
            awaited=event.awaited,
        )

    def succeeded(self, event):
        self._writer.write(
            "heartbeat", "succeeded",
            server=f"{event.connection_id[0]}:{event.connection_id[1]}",
            awaited=event.awaited,
            duration_ms=round(event.duration * 1000.0, 1),
        )

    def failed(self, event):
        self._writer.write(
            "heartbeat", "failed",
            server=f"{event.connection_id[0]}:{event.connection_id[1]}",
            awaited=event.awaited,
            duration_ms=round(event.duration * 1000.0, 1),
            error=str(event.reply),
        )


class _SdamServerListener(monitoring.ServerListener):
    """Logs when the driver's view of ONE specific server's state changes
    (e.g. healthy mongos -> Unknown/unreachable, and back) - the exact
    moment a dead server gets demoted (or a recovered one reinstated) in
    the driver's topology, for direct comparison against our own
    attempt/retry timestamps and the heartbeat/command logs above.
    """

    def __init__(self, writer):
        self._writer = writer

    def opened(self, event):
        self._writer.write("server", "opened", address=f"{event.server_address[0]}:{event.server_address[1]}")

    def description_changed(self, event):
        self._writer.write(
            "server", "description_changed",
            previous=_server_desc_fields(event.previous_description),
            new=_server_desc_fields(event.new_description),
        )

    def closed(self, event):
        self._writer.write("server", "closed", address=f"{event.server_address[0]}:{event.server_address[1]}")


class _SdamTopologyListener(monitoring.TopologyListener):
    """Logs changes to the driver's OVERALL topology view (e.g. which
    server is currently primary/writable, member count) - coarser-grained
    than _SdamServerListener, useful for seeing the end-to-end effect of
    a regional outage on the whole seed list at once.
    """

    def __init__(self, writer):
        self._writer = writer

    def opened(self, event):
        self._writer.write("topology", "opened")

    def description_changed(self, event):
        self._writer.write(
            "topology", "description_changed",
            previous=[_server_desc_fields(d) for d in event.previous_description.server_descriptions().values()],
            new=[_server_desc_fields(d) for d in event.new_description.server_descriptions().values()],
        )

    def closed(self, event):
        self._writer.write("topology", "closed")


def setup_sdam_logging(path, log_command_successes):
    """Register all four listener types globally - MUST be called before
    constructing the MongoClient whose events you want to observe;
    pymongo.monitoring.register() applies to every MongoClient created
    afterward in this process, not retroactively to ones already built.
    Returns the _SdamWriter so the caller can close() it on exit.
    """
    writer = _SdamWriter(path)
    monitoring.register(_SdamCommandListener(writer, log_command_successes))
    monitoring.register(_SdamHeartbeatListener(writer))
    monitoring.register(_SdamServerListener(writer))
    monitoring.register(_SdamTopologyListener(writer))
    return writer


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


def build_flow_models(db_name, collection_name, account_collection_name,
                       outbox_collection_name, hostname, run_id, seq,
                       write_ts_ms, task_id, account_id, task_outbox_id,
                       account_outbox_id):
    """Build the four InsertOne models for one task/account/outbox flow.
    Called fresh on every retry attempt, but always with the SAME four
    _ids, so a retry after an uncertain commit can be detected as a
    duplicate rather than silently writing a second copy.
    """
    return [
        pymongo.InsertOne(
            {
                "_id": task_id,
                "seq": seq,
                "run_id": run_id,
                "write_ts_ms": write_ts_ms,
                "host_created": hostname,
                "source": "load_producer.py",
            },
            namespace=f"{db_name}.{collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": task_outbox_id,
                "aggregate_type": "task",
                "aggregate_id": task_id,
                "event_type": "TASK_CREATED",
                "payload": {"task_id": task_id, "seq": seq, "run_id": run_id},
                "status": "PENDING",
            },
            namespace=f"{db_name}.{outbox_collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": account_id,
                "seq": seq,
                "run_id": run_id,
                "host_created": hostname,
            },
            namespace=f"{db_name}.{account_collection_name}",
        ),
        pymongo.InsertOne(
            {
                "_id": account_outbox_id,
                "aggregate_type": "account",
                "aggregate_id": account_id,
                "event_type": "ACCOUNT_CREATED",
                "payload": {"account_id": account_id, "seq": seq, "run_id": run_id},
                "status": "PENDING",
            },
            namespace=f"{db_name}.{outbox_collection_name}",
        ),
    ]


def insert_one_flow(client, db_name, collection_name, account_collection_name,
                     outbox_collection_name, hostname, run_id, seq,
                     timeout_s=5.0, max_retries=None):
    """Insert one four-document flow (task/task-outbox/account/account-outbox)
    as a single transaction - one bulk_write() server call across all
    four namespaces, wrapped in session.with_transaction() - timing the
    whole (possibly retried) attempt. Runs in a worker thread; pymongo's
    MongoClient is thread-safe and pools connections internally, so
    sharing `client` across threads is safe.

    Uses the transaction's own majority write concern plus a CLIENT-side
    deadline (PyMongo's CSOT `pymongo.timeout()`) plus explicit classic
    socket timeouts set on the MongoClient itself. These cover three
    different failure modes:

    1. majority write concern + `max_commit_time_ms` (passed to
       with_transaction() below, same value as `timeout_s`) - a
       SERVER-side bound specifically on the COMMIT: once the primary
       has actually received and executed it, this is how long IT
       waits for replica acks before giving up and returning a clean
       WriteConcernError. This is the common, "healthy but replicating
       slowly" case, and firing this gives the nicest, fastest,
       most-informative failure. There's no equivalent server-side
       bound available for the bulk_write/insert phase itself - the
       newer client-level bulk_write() API (MongoDB 8.0+, PyMongo 4.9+)
       doesn't expose a maxTimeMS parameter - so that phase relies on
       (2) and (3) below instead.

    2. `connectTimeoutMS`/`socketTimeoutMS`/`serverSelectionTimeoutMS`
       (set on the MongoClient in main(), all equal to
       `args.write_timeout * 1000`) - a CLIENT-side, per-socket-
       operation hard cap applying uniformly to EVERY network call
       (bulk_write's insert command, the commit, server selection,
       reconnects), independent of CSOT. Added as a deliberate belt-
       and-braces backstop after switching from single-document
       insert_one() to the newer, less battle-tested client-level
       bulk_write()-in-a-transaction observably increased how long it
       took to detect a dead connection during DistKafka's regional-
       failover testing - i.e. CSOT alone was not reliably bounding
       BOTH the bulk_write and the commit to the same tight deadline.
       Explicit, low, classic socket timeouts on the client remove any
       dependence on CSOT correctly propagating its deadline through
       every code path inside with_transaction()'s retry/pin/commit
       machinery, guaranteeing a hard stop at the socket layer no
       matter which operation is in flight.

    3. CSOT (`pymongo.timeout()`) - a CLIENT-side bound that fires
       regardless of what's happening server-side or even at the
       socket level. This is what actually saves us in the failure
       mode observed during DistKafka's regional-failover testing:
       mongos's lightweight replica-set monitor correctly detected the
       newly-elected primary as healthy within seconds, but its
       separate execution-pool connection to that same primary was
       wedged (killed mid-flight by the simulated outage, no clean TCP
       FIN/RST) - a write sent over it just hung with no response at
       all. A plain client-side socket read in that situation blocks
       until the OS's own TCP retransmission logic eventually gives up
       (tens of minutes, not configurable here) - confirmed live:
       pointing PyMongo at a deliberately blackholed address,
       `pymongo.timeout(5)` reliably raised after exactly 5.00s
       regardless. Kept alongside (2) since it also bounds the ENTIRE
       with_transaction() call (bulk_write + commit together, plus any
       internal retries pymongo's transaction machinery performs on its
       own) to the same total deadline, not just each individual
       socket operation.

    `session.with_transaction()` already retries internally on
    TransientTransactionError/UnknownTransactionCommitResult, but only
    within whatever deadline is in effect - the outer loop below
    re-tries the ENTIRE transaction (fresh session, same four _ids) if
    the CSOT deadline is hit: if an earlier attempt's commit actually
    landed but we just timed out waiting to hear back, the retry hits
    a DuplicateKeyError on the task document instead of silently
    inserting a second copy of the whole flow - at which point we know
    the original attempt succeeded and stop.

    Returns (seq, mongo_id, write_ts_ms, insert_ms, attempts).
    mongo_id is None in the (rare) duplicate-key case, since we don't
    have the prior attempt's server-assigned result to hand back - the
    caller should treat that the same as success for counting purposes
    (the task doc's _id, task_id, is what's authoritative).
    """
    write_ts_ms = time.time() * 1000.0
    task_id = ObjectId()
    account_id = ObjectId()
    task_outbox_id = ObjectId()
    account_outbox_id = ObjectId()

    def build_models():
        return build_flow_models(
            db_name, collection_name, account_collection_name,
            outbox_collection_name, hostname, run_id, seq, write_ts_ms,
            task_id, account_id, task_outbox_id, account_outbox_id,
        )

    attempt = 0
    t_start = time.perf_counter()

    while True:
        attempt += 1
        try:
            def run_transaction(session):
                # write_concern is NOT passed to bulk_write() here: once a
                # transaction has started, pymongo requires the write
                # concern to come from the transaction itself (passed to
                # with_transaction() below) - passing one on an individual
                # operation inside an active transaction raises
                # InvalidOperation.
                return client.bulk_write(
                    build_models(),
                    ordered=True,
                    session=session,
                )

            with pymongo.timeout(timeout_s):
                with client.start_session() as session:
                    # start_session() itself makes no network call - the
                    # logical session only actually starts with the first
                    # command sent on it (bulk_write's insert, below).
                    session.with_transaction(
                        run_transaction,
                        write_concern=WriteConcern(w="majority"),
                        max_commit_time_ms=int(timeout_s * 1000),
                    )
            insert_ms = (time.perf_counter() - t_start) * 1000.0
            return seq, str(task_id), write_ts_ms, insert_ms, attempt

        except PyMongoError as exc:
            if _is_duplicate_key_error(exc):
                insert_ms = (time.perf_counter() - t_start) * 1000.0
                return seq, str(task_id), write_ts_ms, insert_ms, attempt

            # Covers CSOT deadline expiry (the common case here) as
            # well as any other retryable-ish PyMongo error - either
            # way we don't yet know if the transaction committed, so
            # retry the whole thing (with the same four _ids) rather
            # than give up or duplicate.
            if max_retries is not None and attempt >= max_retries:
                raise
            # No sleep/backoff by design: the CSOT deadline itself
            # already paces retries.
            continue


def main():
    parser = argparse.ArgumentParser(description="Load-test producer")
    parser.add_argument("--count", type=int, default=20000, help="Number of four-document flows to insert")
    parser.add_argument("--run-id", default=None,
                         help="Unique run identifier embedded in each document (default: current epoch ms)")
    parser.add_argument("--concurrency", type=int, default=20,
                         help="Number of concurrent transaction threads (default: 20)")
    parser.add_argument("--log", default="/home/ec2-user/insert_log.csv",
                         help="Path to write the per-insert timing log")
    parser.add_argument("--progress-interval", type=float, default=2.0,
                         help="Seconds between progress prints")
    parser.add_argument("--write-timeout", type=float, default=5.0,
                         help="Client-side (CSOT) seconds to wait per transaction attempt "
                              "before retrying (default: 5.0)")
    parser.add_argument("--max-retries", type=int, default=None,
                         help="Max retry attempts per transaction before giving up "
                              "(default: unlimited)")
    parser.add_argument("--sdam-log", default=None,
                         help="Path to write a JSONL log of PyMongo's SDAM "
                              "(Server Discovery And Monitoring) events and "
                              "command results - server addresses, "
                              "durations, errors, topology/server state "
                              "changes. Diagnostic only; disabled (no "
                              "overhead) unless set. Intended for a live "
                              "failover drill (triggered via "
                              "toggle-mongos-block.sh or Atlas's own "
                              "resilience/fault-injection testing feature), "
                              "to see exactly how long the driver takes to "
                              "detect a dead mongos and fail over, and "
                              "whether it's re-selecting the same dead "
                              "server repeatedly in the meantime.")
    parser.add_argument("--sdam-log-command-successes", action="store_true",
                         help="Also log successful commands to --sdam-log, "
                              "not just failures. Off by default since at "
                              "load-test concurrency/volume this floods the "
                              "log with no diagnostic value for failover "
                              "analysis - only turn on for small, targeted "
                              "drill runs (low --count/--concurrency).")
    args = parser.parse_args()

    load_env()

    mongo_uri = os.environ.get("MONGO_URI")
    db_name = os.environ.get("MONGO_DB_NAME", "bank")
    collection_name = os.environ.get("MONGO_COLLECTION", "tasks")
    account_collection_name = os.environ.get("MONGO_ACCOUNT_COLLECTION", "modular_accounts")
    outbox_collection_name = os.environ.get("MONGO_OUTBOX_COLLECTION", "outbox")
    hostname = socket.gethostname()

    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable not set", file=sys.stderr)
        sys.exit(1)

    run_id = args.run_id or str(int(time.time() * 1000))

    # Must register SDAM/command listeners BEFORE constructing the
    # MongoClient below - pymongo.monitoring.register() only applies to
    # MongoClients created after the call, not retroactively.
    sdam_writer = None
    if args.sdam_log:
        sdam_writer = setup_sdam_logging(args.sdam_log, args.sdam_log_command_successes)
        print(f"SDAM debug logging enabled -> {args.sdam_log}"
              f"{' (including successful commands)' if args.sdam_log_command_successes else ''}",
              flush=True)

    # maxPoolSize must cover the thread pool, or threads will queue up
    # waiting for a connection instead of actually running concurrently.
    #
    # connectTimeoutMS/socketTimeoutMS/serverSelectionTimeoutMS are
    # explicit, LOW classic socket timeouts (independent of CSOT - see
    # insert_one_flow()'s docstring): a hard backstop on every
    # individual socket operation this client performs (bulk_write's
    # insert command, the transaction commit, server selection,
    # reconnects), so detecting a dead/wedged connection can never
    # silently take longer than --write-timeout regardless of which
    # operation (bulk_write vs commit) happens to be in flight when the
    # connection dies.
    write_timeout_ms = int(args.write_timeout * 1000)
    client = MongoClient(
        mongo_uri,
        maxPoolSize=max(args.concurrency, 100),
        connectTimeoutMS=write_timeout_ms,
        socketTimeoutMS=write_timeout_ms,
        serverSelectionTimeoutMS=write_timeout_ms,
    )

    print(f"RUN_ID={run_id}")
    print(f"Client: {os.environ.get('CLIENT_LABEL', 'unknown')}")
    print(f"Inserting {args.count} four-document transactions "
          f"(task={collection_name}, account={account_collection_name}, "
          f"outbox={outbox_collection_name}) in {db_name} "
          f"with {args.concurrency} concurrent workers ...", flush=True)

    try:
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
                        insert_one_flow, client, db_name, collection_name,
                        account_collection_name, outbox_collection_name,
                        hostname, run_id, seq,
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
    finally:
        # Close the client FIRST, before the SDAM log file: this stops
        # its background per-server monitor threads, which otherwise
        # keep running (and can fire a heartbeat callback - a write to
        # sdam_writer) after the file below is closed, racing the
        # process shutdown and raising a spurious "I/O operation on
        # closed file" error from a monitor thread on the way out.
        client.close()
        if sdam_writer:
            sdam_writer.close()
            print(f"Wrote SDAM debug log to {args.sdam_log}")


if __name__ == "__main__":
    main()
