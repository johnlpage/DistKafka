# DistKafka

Infrastructure-as-code for a 5-node MongoDB Atlas M30 cluster across 3 AWS
regions (London x2, Ireland x2, Frankfurt x1), plus two EC2 hosts
(`jpclient-london.mongosa.net`, in the same region as the Atlas primary;
`jpclient-ireland.mongosa.net`, a secondary region)
running Apache Kafka 3.8 and the MongoDB Kafka Source Connector watching
the `bank.payments` collection.

---

## Prerequisites

- Terraform >= 1.8.0
- An existing MongoDB Atlas Project ID
- MongoDB Atlas API keys for the Organization (Programmatic API Keys with
  Project access)

## Required Environment Variables

```bash
# Atlas API credentials (used by the mongodbatlas provider)
export MONGODB_ATLAS_PUBLIC_KEY="<your-public-key>"
export MONGODB_ATLAS_PRIVATE_KEY="<your-private-key>"

# Terraform variables (TF_VAR_ prefix)
export TF_VAR_atlas_project_id="<your-atlas-project-id>"
export TF_VAR_db_password="<database-user-password>"
```

## Deployment

```bash
cd terraform
terraform init
terraform apply
```

The provisioning pipeline runs in three stages:

1. **Bootstrap** — Installs Java + Kafka 3.8 on both EC2 hosts (runs
   concurrently with Atlas cluster creation)
2. **Connector** — Installs MongoDB Kafka Connector + Python dependencies,
   configures it to watch `bank.payments`, starts Kafka Connect
3. **Smoke test** — Uploads `producer.py` and `consumer.py`, inserts a
   test document from each host, and verifies it appears in the Kafka topic

Total deployment time: ~15-20 minutes.

## Verification

After `terraform apply` completes:

```bash
# Check results on the London client (same region as the Atlas primary)
ssh -i terraform/distkafka-ec2-key.pem ec2-user@jpclient-london.mongosa.net
cat /home/ec2-user/kafka_results.csv

# Check results on the Ireland client (secondary Atlas region)
ssh -i terraform/distkafka-ec2-key.pem ec2-user@jpclient-ireland.mongosa.net
cat /home/ec2-user/kafka_results.csv
```

Each CSV should contain 2 rows — one insert from each client.

### Run the test manually

```bash
# On either host:
/home/ec2-user/producer.py           # Insert a document with timestamp
/home/ec2-user/consumer.py           # Read from Kafka, append to CSV
cat /home/ec2-user/kafka_results.csv
```

## Load Testing

Both hosts also get a set of load-test scripts (uploaded automatically by
`terraform apply`, but never run automatically since a full run can take
several minutes):

| Script | Purpose |
|---|---|
| `run_load_test.sh [count] [concurrency]` | Orchestrator - run this one. Purges the Kafka topic, empties the CSV/log files, starts the consumer, runs the producer, waits for the consumer to drain, then prints stats. `count` defaults to `20000`, `concurrency` defaults to `20`. |
| `load_producer.py` | Inserts `count` documents (still one `insert_one()` per document - needed for per-insert timing - but `concurrency` of them in flight at once via a thread pool), embedding `seq` / `run_id` / `write_ts_ms` in each doc. Logs per-insert duration to `insert_log.csv`. Prints progress every 2s. |
| `load_consumer.py` | Consumes the resulting change-stream events from Kafka (fresh consumer group + `latest` offset per run, so old messages never contaminate results), filters by `run_id`, and logs `change_wall_ms` (the change event's own server-side visibility timestamp) and `receipt_ts_ms` per document to `kafka_results.csv`. |
| `compute_load_stats.py` | Joins `insert_log.csv` and `kafka_results.csv` on `seq` and prints sent/received/missing counts plus min/mean/p95/p99/max for three latency phases (see below). |

Run it on either host:

```bash
ssh -i terraform/distkafka-ec2-key.pem ec2-user@jpclient-london.mongosa.net
./run_load_test.sh 20000 20   # 20000 documents, 20 concurrent producer threads
```

Each host's `.env` carries a `CLIENT_LABEL` (e.g. `London (EU_WEST_2) -
same region as Atlas primary` / `Ireland (EU_WEST_1) - secondary Atlas
region`), printed at the top of `run_load_test.sh`'s output and in
`compute_load_stats.py`'s summary header - so proximity to the primary
is obvious in the results without needing to remember which host is
which.

Sample output:

```
=== Load test: run_id=1790680983815 count=20000 concurrency=20 ===
=== Client: London (EU_WEST_2) - same region as Atlas primary ===
Purging existing records from Kafka topic 'bank.payments'...
Consumer ready.
RUN_ID=1790680983815
Client: London (EU_WEST_2) - same region as Atlas primary
Inserting 20000 documents into bank.payments with 20 concurrent workers ...
[t+   12.3s] produced 4032/20000 (327.6/s)
[t+   14.3s] produced 4680/20000 (...)
...
[t+   61.4s] produced 20000/20000 (325.7/s) - DONE

=== Stats ===
================================================================
Load test summary
Client:   London (EU_WEST_2) - same region as Atlas primary
================================================================
Sent:     20000
Received: 20000
Matched:  20000
Missing:  0

Insert time  (client's full insert_one() round-trip):
  n      = 20000
  min    = 13.71 ms
  mean   = 28.08 ms
  p95    = 37.27 ms
  p99    = 330.20 ms
  max    = 756.63 ms

Mongo visibility time  (write -> change-stream visible, per server wallTime):
  n      = 20000
  min    = -0.25 ms
  mean   = 7.96 ms
  p95    = 3.15 ms
  p99    = 314.85 ms
  max    = 735.31 ms

Kafka pipeline time  (change-stream visible -> consumer receipt):
  n      = 20000
  min    = 18.19 ms
  mean   = 48.13 ms
  p95    = 125.33 ms
  p99    = 148.97 ms
  max    = 158.08 ms
================================================================
```

Three latency phases, each measuring something different:

1. **Insert time** - the client's full `insert_one()` round-trip: time to
   send the write, have it majority-committed (Atlas's implicit default
   write concern for replica sets), and receive the ack back.
2. **Mongo visibility time** - `change_wall_ms - write_ts_ms`, where
   `change_wall_ms` comes from the change event's own server-side
   `wallTime` field (the moment the write became majority-committed and
   visible to any change-stream watcher, per the Atlas cluster's own
   clock). Usually near-zero (visibility happens fast), with an
   occasional heavy tail from real replication hiccups.
3. **Kafka pipeline time** - `receipt_ts_ms - change_wall_ms`: purely the
   Kafka Connect -> Kafka broker -> this consumer portion, strictly
   downstream of (2) so it should never legitimately be negative.

**Why not just `(receipt_ts_ms - write_ts_ms) - insert_ms`?** That was
the original (flawed) approach, and it could go negative: part of the
client's `insert_one()` round-trip happens *after* the write is already
majority-committed and visible - specifically, the ack traveling back
across the network to the client - and Kafka Connect's own path to
noticing the change can easily be faster than that return trip.
Anchoring on the change event's own `wallTime` instead of the client's
round-trip time avoids this: it isolates exactly when the write became
visible, independent of how long it then took the ack to get back to
the inserting client.

`write_ts_ms`/`receipt_ts_ms`/`change_wall_ms` are all wall-clock
(`time.time()`/server epoch-ms), which is safe here because these EC2
hosts are tightly NTP-synced via `chronyd` (confirmed to within a few
microseconds via `chronyc tracking`).

Insert timing is per-document and unaffected by concurrency (each thread
times only its own `insert_one()` call); raising `concurrency` mainly
increases overall throughput and can surface connection-pool/contention
effects that a single-threaded run wouldn't show.

The consumer stops itself once it has received `count` messages, or after
30s with no new matching messages (whichever comes first) - the latter is
a safety net in case some messages are lost, so the script can't hang
forever.

### Kafka Connect tuning for latency

`setup-connector.sh` sets `poll.await.time.ms=50` (down from the
connector's default of `5000`) on the MongoDB source connector, so it
doesn't sit idle for up to 5 seconds before returning an empty batch.

`poll.max.batch.size` is deliberately left at its default (`1000`), NOT
lowered - empirically, forcing it down to `1` makes latency *worse*
under concurrent/bursty load: the connector can then only pull one
change-stream event per poll cycle even when hundreds are already
backlogged, turning it into a single-item-at-a-time queueing
bottleneck (measured ~250ms mean pipeline time with `batch.size=1` vs.
~50-130ms with the default `1000`, same `poll.await.time.ms`, same
1000-document/20-thread load). A large batch size lets it drain a
backlog in one go; the low `poll.await.time.ms` keeps it from padding
latency when there's no backlog to drain.

Expect real run-to-run variance in these numbers - this is a live,
shared-tenancy, cross-region Atlas cluster (EU_WEST_2/EU_WEST_1/EU_CENTRAL_1),
not a fixed benchmark environment.

## Customization

| Variable | Default | Description |
|---|---|---|
| `nshards` | `0` | 0 = replicaset, >=1 = sharded (each shard gets a 2 London/2 Dublin/1 Frankfurt layout) |
| `config_server_type` | `"embedded"` | `"embedded"` or `"dedicated"` (sharded only) |
| `atlas_instance_size` | `"M30"` | Atlas tier |
| `db_username` | `"distkafkaApp"` | Database user name |
| `db_name` | `"bank"` | Database (collection: `payments`) |
| `kafka_version` | `"4.3.1"` | Apache Kafka release (KRaft mode) |
| `kafka_read_preference` | `"primary"` | Read preference for the Kafka connector's connection URI |
| `ec2_instance_type` | `"t3.medium"` | EC2 instance type (needs enough RAM for both the Kafka broker and Connect JVMs) |
| `privatelink_enabled` | `true` | Use PrivateLink + cross-region Transit Gateway failover instead of public IP access - see [ARCHITECTURE.md](ARCHITECTURE.md) |

Set overrides in `terraform.tfvars` or as `TF_VAR_*` environment variables.

## Outputs

After apply, run `terraform output` to see:

- `atlas_connection_string_srv` — SRV connection string for the Atlas cluster
- `atlas_private_connection_strings_srv` — PrivateLink SRV connection string(s), when available
- `jpclient_london_public_ip` / `jpclient_ireland_public_ip` — Elastic IPs
- `jpclient_london_hostname` / `jpclient_ireland_hostname` — DNS hostnames
- `ssh_command_london` / `ssh_command_ireland` — SSH commands (with
  local port forwarding for Kafka 9092 and Connect 8083)

## PrivateLink

Enabled by default (`privatelink_enabled = true`, EC2 hosts connect through
AWS PrivateLink). To opt out and use public IP access instead:

```bash
TF_VAR_privatelink_enabled=false terraform apply
```

This replaces the public IP access list with AWS PrivateLink, and forces
the cluster to sharded (embedded config servers, 1 shard by default -
see `var.nshards`) because the failover design below relies on `mongos`
query routers, which only exist for sharded clusters.

### What gets created

- Atlas-side PrivateLink services in London (`EU_WEST_2`), Dublin/Ireland
  (`EU_WEST_1`), and Frankfurt (`EU_CENTRAL_1`). Frankfurt is consumed by
  a cross-region interface endpoint hosted in the Dublin VPC.
- AWS resources are created only in the London and Dublin VPCs. No Frankfurt
  AWS VPC, subnet, security group, or provider is used.
- The Dublin-hosted Frankfurt endpoint uses AWS `service_region =
  "eu-central-1"`; the Frankfurt Atlas service allows `EU_WEST_1` as its
  supported remote endpoint region.
- **Cross-region failover**: London's and Dublin's VPCs each get local
  interface endpoints, and the Dublin VPC also consumes the Frankfurt Atlas
  service through AWS cross-region PrivateLink. The existing London/Dublin
  Transit Gateway link remains available for cross-region network access.
- Security groups allowing inbound **1024-65535** (not just 27017) on
  each endpoint - Atlas's PrivateLink load balancer multiplexes each
  cluster node onto a different port on the same private IP.

### Automatic cross-region failover

Both hosts' `.env` get the **same combined connection string**: every
`mongos` router reachable from either region (own-region endpoint +
cross-region endpoint) is combined into one seed list
(`provisions.tf`'s `mongo_uri_combined_private`). This is safe
specifically *because* `mongos` routers are stateless query routers -
the MongoDB driver just uses whichever ones are currently reachable,
with no risk of "forgetting" one region's hosts the way there would be
with raw replica-set members (where each node's `hello`/`isMaster`
response drives topology rediscovery in a way Atlas customizes
per-PrivateLink-endpoint). This applies to the Kafka Source Connector
too, since it builds its `MongoClient` directly from `connection.uri` -
no supervisor process or custom retry logic needed anywhere.

**Not yet empirically verified**: this combined-seed-list-across-two-
PrivateLink-endpoints pattern isn't an officially documented MongoDB
pattern (though it rests on standard, supported driver mechanics). If
you rely on this for real failover, test it first - e.g. temporarily
strip one region's security group ingress rule mid-load-test and
confirm the driver actually keeps working via the other region rather
than hanging or erroring.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design document.
