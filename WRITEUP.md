# DistKafka: Cross-Region Resilience Test

## Setup
- This is all available as a Terraform script to deploy and all code is python - heppy to demonstrate and/or give access if a local setup is problematic.
- Atlas sharded cluster spanning **London, Dublin, Frankfurt**. Sharded (not a plain replica set) to make regional resilience easier; embedded config servers for lower cost/latency.
- Every shard: 2 nodes London (priority 7), 2 Dublin (priority 6), 1 Frankfurt (priority 5).
- Client access via **Atlas PrivateLink** only (no public IP path). London↔Dublin cross-region **Transit Gateway**  gives automatic failover to Dublin's endpoint if London's is unreachable.
- Kafka + MongoDB Source Connector run locally on both London and Dublin EC2 hosts, each using the same combined `mongodb+srv://` seed list (all regions' mongos routers).
- Producer: concurrent threads inserting on the London host. Consumers: read from the Kafka topic, log write and receipt timestamps.

## Write reliability design
Every insert uses `writeConcern: {w: "majority"}` with a bounded wait, and retries automatically if that bound is exceeded:

- A fixed timeout (default 5s) is enforced on each attempt, covering both a slow-replication case and a fully unresponsive connection.
- On timeout, the **same document** (fixed `_id`) is retried - never a fresh one.
- If a retry hits a duplicate-key error, that means an earlier attempt already succeeded; the producer stops retrying and treats it as success rather than inserting a second copy.

This guarantees: no lost writes, no duplicate writes, and automatic recovery without manual intervention once the underlying connection is restored. Although these are mostly automatic this helps where the problem is a write and then waiting for a return because the network has dropped but the client cannot tell - this is endge case when the region goes offline mid write.

## Results

**Normal operation:**
- Average write (insert) latency: **~25ms**
- End-to-end (write → oplog → Kafka → consumer): **~90–110ms** (±10%)

**Regional failover (London down, Dublin takes over):**
- Zero data loss and zero duplicate documents.
- Writes in flight during the failover automatically retry until the connection to the new primary is available, then complete normally with no application-level errors surfaced and no manual intervention required.
- Writes delayed up to 30s during failover of private networking (unlike 2-5 seconds for primary failure)
