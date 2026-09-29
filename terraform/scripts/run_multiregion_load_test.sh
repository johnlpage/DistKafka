#!/bin/bash
# Multi-region load-test orchestrator: run this from your LAPTOP (not
# deployed to the EC2 hosts). Coordinates a single producer run on the
# London host with TWO simultaneous consumers - one on London (same
# region as the Atlas primary), one on Ireland (secondary region) -
# each against their own independent local Kafka broker/connector
# stack, so you get a direct side-by-side comparison of same-region vs
# cross-region consumer latency for the exact same stream of writes.
#
# Usage:
#   ./run_multiregion_load_test.sh [count] [concurrency]
#     count       default: 20000  (documents inserted on London)
#     concurrency default: 20     (concurrent producer threads on London)
#
# Requires:
#   - ssh access to both hosts (uses terraform output + the generated
#     key, so run this from a machine that has both)
#   - local python3 (stdlib only - csv/argparse/statistics - no extra
#     packages needed) to compute the final stats
#
# Steps:
#   1. Purges each host's own Kafka topic
#   2. Starts load_consumer.py in the background on BOTH hosts (fresh
#      consumer group + auto_offset_reset=latest per host), waits for
#      both to confirm readiness (polls the locally-redirected log for
#      the "Consumer ready" line - no extra SSH round trips needed)
#   3. Runs load_producer.py synchronously on London only
#   4. Waits for both consumers to drain/finish
#   5. Pulls back insert_log.csv (London) + both hosts' kafka_results.csv
#   6. Runs compute_load_stats.py locally, once per region, against the
#      shared insert_log so the two are directly comparable
set -euo pipefail

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_KEY="${TF_DIR}/distkafka-ec2-key.pem"
COUNT="${1:-20000}"
CONCURRENCY="${2:-20}"
RUN_ID="$(date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')"

LONDON_IP="$(terraform -chdir="${TF_DIR}" output -raw jpclient_london_public_ip)"
IRELAND_IP="$(terraform -chdir="${TF_DIR}" output -raw jpclient_ireland_public_ip)"

RESULTS_DIR="${TF_DIR}/results/multiregion_${RUN_ID}"
mkdir -p "${RESULTS_DIR}"

SSH="ssh -o StrictHostKeyChecking=no -i ${SSH_KEY}"

echo "=== Multi-region load test: run_id=${RUN_ID} count=${COUNT} concurrency=${CONCURRENCY} ==="
echo "London:  ${LONDON_IP}  (producer + same-region consumer)"
echo "Ireland: ${IRELAND_IP}  (cross-region consumer only)"
echo "Results: ${RESULTS_DIR}"
echo

purge_topic() {
  local ip="$1" label="$2"
  echo "Purging Kafka topic on ${label} (${ip})..."
  ${SSH} ec2-user@"${ip}" bash -s <<'REMOTE'
set -euo pipefail
source /home/ec2-user/.env
TOPIC="${KAFKA_TOPIC:-bank.payments}"
KAFKA_DIR="/opt/kafka"
PARTITIONS=$("${KAFKA_DIR}/bin/kafka-topics.sh" --describe --topic "${TOPIC}" --bootstrap-server localhost:9092 \
  | head -1 | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+')
PARTITIONS="${PARTITIONS:-1}"
JSON="/tmp/purge_${TOPIC//[^A-Za-z0-9]/_}.json"
{
  printf '{"version":1,"partitions":['
  for ((p = 0; p < PARTITIONS; p++)); do
    [ "$p" -gt 0 ] && printf ','
    printf '{"topic":"%s","partition":%d,"offset":-1}' "${TOPIC}" "$p"
  done
  printf ']}'
} > "${JSON}"
"${KAFKA_DIR}/bin/kafka-delete-records.sh" --bootstrap-server localhost:9092 --offset-json-file "${JSON}"
rm -f "${JSON}"
REMOTE
}

purge_topic "${LONDON_IP}" "London"
purge_topic "${IRELAND_IP}" "Ireland"
echo

# --- 2. Start both consumers in the background ---------------------------

LONDON_CONSUMER_LOG="${RESULTS_DIR}/london_consumer.log"
IRELAND_CONSUMER_LOG="${RESULTS_DIR}/ireland_consumer.log"

echo "Starting London (same-region) consumer..."
${SSH} ec2-user@"${LONDON_IP}" \
  "./load_consumer.py --run-id ${RUN_ID} --expected ${COUNT} --csv kafka_results.csv --ready-file /tmp/ready_${RUN_ID} --idle-timeout 30" \
  > "${LONDON_CONSUMER_LOG}" 2>&1 &
LONDON_CONSUMER_PID=$!

echo "Starting Ireland (cross-region) consumer..."
${SSH} ec2-user@"${IRELAND_IP}" \
  "./load_consumer.py --run-id ${RUN_ID} --expected ${COUNT} --csv kafka_results.csv --ready-file /tmp/ready_${RUN_ID} --idle-timeout 30" \
  > "${IRELAND_CONSUMER_LOG}" 2>&1 &
IRELAND_CONSUMER_PID=$!

wait_for_ready() {
  local logfile="$1" pid="$2" label="$3"
  for _ in $(seq 1 60); do
    if grep -q "Consumer ready" "${logfile}" 2>/dev/null; then
      echo "${label} consumer ready."
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "ERROR: ${label} consumer died before becoming ready. Log:"
      cat "${logfile}"
      exit 1
    fi
    sleep 0.5
  done
  echo "ERROR: ${label} consumer did not become ready in time. Log:"
  cat "${logfile}"
  exit 1
}

wait_for_ready "${LONDON_CONSUMER_LOG}" "${LONDON_CONSUMER_PID}" "London"
wait_for_ready "${IRELAND_CONSUMER_LOG}" "${IRELAND_CONSUMER_PID}" "Ireland"
echo

# --- 3. Run the producer on London, synchronously -------------------------

echo "Running producer on London (${COUNT} documents, ${CONCURRENCY} concurrent workers)..."
${SSH} ec2-user@"${LONDON_IP}" \
  "./load_producer.py --count ${COUNT} --run-id ${RUN_ID} --concurrency ${CONCURRENCY} --log insert_log.csv" \
  | tee "${RESULTS_DIR}/producer.log"
echo

# --- 4. Wait for both consumers to drain/finish ---------------------------

echo "Producer finished. Waiting for both consumers to drain remaining messages..."
set +e
wait "${LONDON_CONSUMER_PID}"; LONDON_CONSUMER_EXIT=$?
wait "${IRELAND_CONSUMER_PID}"; IRELAND_CONSUMER_EXIT=$?
set -e

echo "--- London consumer log ---"
cat "${LONDON_CONSUMER_LOG}"
echo "--- Ireland consumer log ---"
cat "${IRELAND_CONSUMER_LOG}"

for pair in "London:${LONDON_CONSUMER_EXIT}" "Ireland:${IRELAND_CONSUMER_EXIT}"; do
  label="${pair%%:*}"
  exit_code="${pair##*:}"
  if [ "${exit_code}" -eq 2 ]; then
    echo "NOTE: ${label} consumer stopped early on its idle timeout - some messages may be missing."
  elif [ "${exit_code}" -ne 0 ]; then
    echo "WARNING: ${label} consumer exited with code ${exit_code}"
  fi
done
echo

# --- 5. Pull results back locally ------------------------------------------

echo "Fetching results..."
scp -o StrictHostKeyChecking=no -i "${SSH_KEY}" ec2-user@"${LONDON_IP}":insert_log.csv "${RESULTS_DIR}/insert_log.csv"
scp -o StrictHostKeyChecking=no -i "${SSH_KEY}" ec2-user@"${LONDON_IP}":kafka_results.csv "${RESULTS_DIR}/kafka_results_london.csv"
scp -o StrictHostKeyChecking=no -i "${SSH_KEY}" ec2-user@"${IRELAND_IP}":kafka_results.csv "${RESULTS_DIR}/kafka_results_ireland.csv"
echo

# --- 6. Compute stats locally, once per region -----------------------------

echo "############################################################"
echo "# LONDON CONSUMER (same region as Atlas primary)"
echo "############################################################"
python3 "${TF_DIR}/scripts/compute_load_stats.py" \
  --insert-log "${RESULTS_DIR}/insert_log.csv" \
  --receipt-log "${RESULTS_DIR}/kafka_results_london.csv" \
  --client-label "London consumer (same region as Atlas primary)" \
  --run-id "${RUN_ID}" \
  --json-out "${RESULTS_DIR}/results_${RUN_ID}_london.json"

echo
echo "############################################################"
echo "# IRELAND CONSUMER (secondary Atlas region)"
echo "############################################################"
python3 "${TF_DIR}/scripts/compute_load_stats.py" \
  --insert-log "${RESULTS_DIR}/insert_log.csv" \
  --receipt-log "${RESULTS_DIR}/kafka_results_ireland.csv" \
  --client-label "Ireland consumer (secondary Atlas region)" \
  --run-id "${RUN_ID}" \
  --json-out "${RESULTS_DIR}/results_${RUN_ID}_ireland.json"

echo
echo "Raw CSVs and logs saved in: ${RESULTS_DIR}"
