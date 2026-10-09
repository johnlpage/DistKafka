# Manual Load Test Process

This is a manual, no-automation version of `terraform/scripts/run_multiregion_load_test.sh`.
It is for someone running the load test by hand from terminals on their own
EC2 instance(s), talking to a **Kafka broker running in a local Docker
container** instead of the Terraform-provisioned systemd/native Kafka
install. There is no SSH orchestration, no `scp`, no stats-gathering
automation - you drive each script yourself and run `compute_load_stats.py`
at the end.

It covers two different things that live in this repo:

1. **The Kafka load test** (`load_producer.py` + `load_consumer.py` +
   `compute_load_stats.py`) - requires a Kafka broker + the MongoDB Kafka
   Source Connector in front of Atlas.
2. **`outbox_producer.py`** (top-level, not in `terraform/scripts/`) - a
   much simpler standalone MongoDB bulk-write benchmark. **It does not work
   the same way** - see the dedicated section at the end.

---

## 1. What you need

- Files: `load_producer.py`, `load_consumer.py`, `compute_load_stats.py`
  (all in `terraform/scripts/`). Copy these three files to the EC2
  instance (e.g. `scp`, `git clone`, or just paste them) - they're plain
  scripts, nothing else from the Terraform tree is required.
- Python 3 with:
  ```bash
  pip3 install pymongo kafka-python
  ```
- A Kafka broker reachable from the instance (your local Docker
  container), with the **MongoDB Kafka Source Connector** already
  configured and running against it, watching the task collection below.
  This manual process does not start Kafka or the connector for you -
  only the producer/consumer/stats scripts.
- A MongoDB Atlas (or any replica-set/sharded) connection string.

## 2. Environment variables

The scripts read these from the process environment, or from a `.env`
file at `/home/ec2-user/.env` by default (override the path if you're not
using that user - see "Changing the .env path" below). Format is simple
`KEY="value"` lines, one per line, `#` comments allowed - this is the
same format Terraform writes via `terraform/templates/env.tpl`:

```bash
# /home/ec2-user/.env  (or export these directly in your shell instead)

# --- Required ---
MONGO_URI="mongodb+srv://user:pass@yourcluster.mongodb.net/?retryWrites=true&w=majority"

# --- Used by load_producer.py / load_consumer.py, with these defaults if unset ---
MONGO_DB_NAME="bank"                    # default: bank
MONGO_COLLECTION="tasks"                # default: tasks   (the collection the connector watches)
MONGO_ACCOUNT_COLLECTION="modular_accounts"   # default: modular_accounts
MONGO_OUTBOX_COLLECTION="outbox"        # default: outbox
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"  # default: localhost:9092 - point this at wherever
                                           # your Docker Kafka container publishes 9092
KAFKA_TOPIC="bank.tasks"                # default: bank.tasks - must match the connector's
                                         # configured output topic for MONGO_COLLECTION

# --- Cosmetic only, printed in script/stats output ---
CLIENT_LABEL="My Docker test host"
```

If you'd rather not create a `.env` file, just `export` the same variables
in your shell before running the scripts - both methods work
(`os.environ.setdefault` means real exported env vars always win over the
`.env` file if both are set).

### Changing the `.env` path

All three scripts hard-code `/home/ec2-user/.env` as the default path
they look for. If your instance doesn't use `ec2-user`, either:
- put the file at that exact path anyway (simplest), or
- `export` the variables directly instead of using a `.env` file at all.

(There's no `--env-file` flag - it's not worth patching the scripts for
this; exporting the variables is equivalent and less fuss.)

## 3. Kafka topic housekeeping (optional, recommended)

For clean, comparable results each run, purge the topic before starting
so old messages from a previous run can't be mistaken for new ones (the
consumer's `run_id` filter already protects correctness, this is just
cleanliness). How you do this depends on how you're running the Kafka CLI
tools against your Docker broker - two common options:

**Option A - CLI tools installed on the host, pointed at the container's published port:**
```bash
kafka-topics.sh --describe --topic bank.tasks --bootstrap-server localhost:9092
kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file purge.json
```

**Option B - run the CLI tools inside the broker container:**
```bash
docker exec -it <your-kafka-container-name> \
  kafka-topics.sh --describe --topic bank.tasks --bootstrap-server localhost:9092
```

The purge itself needs a small JSON file describing each partition to
truncate to offset -1 (i.e. delete everything). For a single-partition
topic:
```bash
cat > purge.json <<'EOF'
{"version":1,"partitions":[{"topic":"bank.tasks","partition":0,"offset":-1}]}
EOF
kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file purge.json
```
Add one more `{"topic":...,"partition":N,"offset":-1}` entry per extra
partition if your topic has more than one.

This step is optional - `auto_offset_reset="latest"` plus the fresh
consumer group `load-test-<run-id>` on every run means stale messages
can't inflate your stats even if you skip it, but the topic will grow
unbounded across repeated runs if you never purge.

## 4. Running the test - three terminals

You'll need three terminal sessions on the instance (or three panes/tmux
windows): one for the consumer, one for the producer, one free for
signalling "stop" and running the stats afterwards. Pick a `RUN_ID`
up front and use the *same* value everywhere below - it's just a string,
e.g. the current epoch-ms:

```bash
RUN_ID=$(date +%s%3N)
echo "RUN_ID=${RUN_ID}"
```

### Terminal 1 - start the consumer (run this first, leave it running)

```bash
./load_consumer.py \
  --run-id "${RUN_ID}" \
  --expected 20000 \
  --csv kafka_results.csv \
  --ready-file /tmp/ready_${RUN_ID} \
  --stop-file /tmp/stop_${RUN_ID}
```

- `--expected` is just used for progress printing (`received N/expected`)
  - it does not stop the consumer.
- Wait for it to print `Consumer ready on topic '...'` before moving on -
  this means it actually has a partition assignment and won't miss
  messages the producer is about to send.
- It keeps running (printing progress every 2s) until you create the
  `--stop-file` path from Terminal 3, below. **It will not exit on its
  own** - don't just Ctrl-C it, or it won't print its final summary line
  (the receipt CSV itself is flushed row-by-row so no data is lost
  either way, but you'll lose the max-gap/received-count summary).

### Terminal 2 - run the producer (after the consumer says "ready")

```bash
./load_producer.py \
  --count 20000 \
  --run-id "${RUN_ID}" \
  --concurrency 20 \
  --log insert_log.csv
```

- `--count` and `--concurrency` must match what you told the consumer to
  expect (well, `--count` should match `--expected`; concurrency only
  affects throughput).
- `--run-id` **must** match Terminal 1's `RUN_ID` exactly, or the
  consumer's `run_id` filter will silently ignore every message.
- Optional: add `--sdam-log sdam_producer.jsonl` to also capture PyMongo
  driver-internals diagnostics (server health-check timing, topology
  changes, command failures) - useful if you're investigating
  failover/reconnect behavior rather than just steady-state latency. See
  `./load_producer.py --help` for more on this and on `--write-timeout`
  / `--max-retries`.
- This runs in the foreground and exits on its own once all `--count`
  transactions are done (or fails loudly if something's badly wrong).

### Terminal 3 - after the producer finishes

Wait a few seconds after Terminal 2 exits (to let the consumer drain any
messages still in flight through Kafka Connect), then signal the
consumer to stop:

```bash
sleep 5
touch /tmp/stop_${RUN_ID}
```

Terminal 1 will notice the stop-file on its next poll cycle (sub-second)
and print its final summary (`received N/expected - STOPPED BY EXPLICIT
SIGNAL`, plus the max-gap-between-messages line) before exiting.

### Terminal 3 (continued) - compute stats

Once Terminal 1 has exited:

```bash
python3 compute_load_stats.py \
  --insert-log insert_log.csv \
  --receipt-log kafka_results.csv \
  --run-id "${RUN_ID}" \
  --json-out results_${RUN_ID}.json
```

This joins the two CSVs on `seq` and prints sent/received/missing counts
plus min/mean/p95/p99/max for insert time, Mongo visibility time, Kafka
pipeline time, and total create-to-receipt time (see the main
[README.md](README.md#load-testing) for what each phase means). It also
reads `CLIENT_LABEL` from your `.env`/environment for the printed header
- override with `--client-label "..."` if you'd rather not rely on that.

## 5. Running it on two hosts for a same-region vs. cross-region comparison

If you have two separate EC2 instances (each with its own Docker Kafka
broker + connector against the same Atlas cluster, one same-region as the
Atlas primary and one cross-region) and want the London-vs-Ireland style
side-by-side comparison that `run_multiregion_load_test.sh` automates:

1. Pick one `RUN_ID` and use it on **both** hosts.
2. Start `load_consumer.py` on **both** hosts (Terminal 1 on each),
   each writing to its own local `kafka_results.csv`.
3. Run `load_producer.py` on **only one** host (the "writer" host) -
   both consumers will see the same stream of Kafka messages (assuming
   each host's connector is independently watching the same Atlas
   collection), so you get two independent receipt logs for the same
   writes.
4. Stop both consumers (same `/tmp/stop_<run-id>` trick, done on each
   host separately against its own stop-file path).
5. Copy both hosts' `kafka_results.csv` and the producer host's
   `insert_log.csv` to wherever you're computing stats (same machine,
   `scp`, shared mount, whatever's convenient).
6. Run `compute_load_stats.py` **twice**, once per host, both times
   against the *same* `insert_log.csv` but each host's own
   `kafka_results.csv`, with a `--client-label` identifying which is
   which - exactly as `run_multiregion_load_test.sh` does in its final
   step.

## 6. Does `outbox_producer.py` work the same way?

**No - it's a different, much simpler tool.** It lives at the repo root
(not in `terraform/scripts/`), and:

- **It does not talk to Kafka at all.** No `KAFKA_BOOTSTRAP_SERVERS`, no
  `KAFKA_TOPIC`, no consumer, no change streams, no connector needed.
- **It's not a transaction.** It does the same four-document
  `bulk_write()` (task / task-outbox / account / account-outbox) as
  `load_producer.py`, but as a plain ordered bulk write with
  `write_concern=WriteConcern(w="majority")`, *not* wrapped in
  `session.with_transaction()`. A partial failure (e.g. doc 3 of 4 fails)
  can leave a partial set of documents behind - it says so in its own
  docstring.
- **It cleans up after itself.** At the end of the run it
  `delete_many()`s every document it inserted (by the IDs it generated),
  so repeated runs don't leave junk behind. `load_producer.py`, by
  contrast, leaves its documents in place (they're the whole point - the
  consumer needs them).
- **It measures only the bulk-write latency itself**, timed client-side
  (`time.perf_counter()` around the `bulk_write()` call) - there's no
  "visibility time" or "Kafka pipeline time" split, since there's no
  change-stream/Kafka leg to measure.

It only needs `MONGO_URI` (same `.env`-at-`/home/ec2-user/.env` loading
convention, same quoting rules). Usage:

```bash
./outbox_producer.py --count 1000 --threads 20 --timeout 30 --log latencies.csv
```

| Flag | Default | Meaning |
|---|---|---|
| `--count` | 1000 | Number of four-document bulk writes |
| `--threads` | 20 | Concurrent worker threads |
| `--timeout` | 30.0 | Per-bulk-write client-side (CSOT) timeout, seconds |
| `--run-id` | epoch-ms | Embedded in generated document IDs, not otherwise used |
| `--log` | (none) | Optional CSV path to dump the raw per-write latency list |

Output is a single printed summary (throughput, mean/p50/p95/p99/max
latency) - no separate stats-computation step needed, and nothing to
join against a Kafka receipt log since there isn't one.

**Bottom line:** use `outbox_producer.py` if you just want a quick,
Kafka-free "how fast can Atlas accept this four-document write shape"
number. Use `load_producer.py` + `load_consumer.py` +
`compute_load_stats.py` (sections 1-5 above) if you want the full
write-to-Kafka-receipt pipeline latency, including the Docker Kafka
broker and the Source Connector in the loop.
