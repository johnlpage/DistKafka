#!/bin/bash
# Load-test orchestrator: run this directly on jpclient1 or jpclient2.
#
# Usage:
#   ./run_load_test.sh [count] [concurrency]
#     count       default: 20000
#     concurrency default: 20  (number of concurrent producer threads)
#
# Steps:
#   (a) empties kafka_results.csv, insert_log.csv, AND the Kafka topic
#       itself (via kafka-delete-records.sh, so old records from previous
#       runs can't inflate the topic or confuse anyone poking around
#       manually - the consumer's run_id filter already protects the
#       stats themselves, this is belt-and-braces plus keeps the topic
#       from growing unbounded across repeated runs)
#   (b) starts load_consumer.py in the background (fresh consumer group,
#       auto_offset_reset=latest) and waits for it to confirm it has a
#       partition assignment before continuing, so no early messages
#       are missed
#   (c) runs load_producer.py synchronously, inserting `count` documents
#       (still one insert_one() per document, `concurrency` in flight at
#       once) and printing progress every 2s
#   (d) waits for the consumer to drain the remaining in-flight messages
#       (it stops itself once it has received `count` messages, or after
#       30s of no new matching messages)
#   (e) prints final latency stats (insert time, and Mongo-write ->
#       Kafka-receipt transit time)
set -euo pipefail

HOME_DIR="/home/ec2-user"
KAFKA_DIR="/opt/kafka"
COUNT="${1:-20000}"
CONCURRENCY="${2:-20}"
RUN_ID="$(date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')"

CSV="${HOME_DIR}/kafka_results.csv"
INSERT_LOG="${HOME_DIR}/insert_log.csv"
CONSUMER_LOG="${HOME_DIR}/load_consumer.log"
READY_FILE="/tmp/load_consumer_ready_${RUN_ID}"

# shellcheck disable=SC1091
source "${HOME_DIR}/.env"
TOPIC="${KAFKA_TOPIC:-bank.payments}"

echo "=== Load test: run_id=${RUN_ID} count=${COUNT} concurrency=${CONCURRENCY} ==="
echo "=== Client: ${CLIENT_LABEL:-unknown} ==="

# (a) empty the csv/log files and purge the Kafka topic
: > "${CSV}"
: > "${INSERT_LOG}"
rm -f "${READY_FILE}"

echo "Purging existing records from Kafka topic '${TOPIC}'..."
PARTITIONS=$("${KAFKA_DIR}/bin/kafka-topics.sh" --describe --topic "${TOPIC}" --bootstrap-server localhost:9092 \
  | head -1 | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+')
PARTITIONS="${PARTITIONS:-1}"

PURGE_JSON="/tmp/purge_${TOPIC//[^A-Za-z0-9]/_}_${RUN_ID}.json"
{
  printf '{"version":1,"partitions":['
  for ((p = 0; p < PARTITIONS; p++)); do
    [ "$p" -gt 0 ] && printf ','
    printf '{"topic":"%s","partition":%d,"offset":-1}' "${TOPIC}" "$p"
  done
  printf ']}'
} > "${PURGE_JSON}"

"${KAFKA_DIR}/bin/kafka-delete-records.sh" --bootstrap-server localhost:9092 --offset-json-file "${PURGE_JSON}"
rm -f "${PURGE_JSON}"

# (b) start consumer in background
python3 "${HOME_DIR}/load_consumer.py" \
  --run-id "${RUN_ID}" \
  --expected "${COUNT}" \
  --csv "${CSV}" \
  --ready-file "${READY_FILE}" \
  --idle-timeout 30 \
  > "${CONSUMER_LOG}" 2>&1 &
CONSUMER_PID=$!
echo "Consumer PID: ${CONSUMER_PID} (log: ${CONSUMER_LOG})"

echo "Waiting for consumer to be ready..."
READY=0
for i in $(seq 1 60); do
  if [ -f "${READY_FILE}" ]; then
    READY=1
    break
  fi
  if ! kill -0 "${CONSUMER_PID}" 2>/dev/null; then
    echo "ERROR: consumer process died before becoming ready. Log:"
    cat "${CONSUMER_LOG}"
    exit 1
  fi
  sleep 0.5
done

if [ "${READY}" -ne 1 ]; then
  echo "ERROR: consumer did not become ready in time. Log:"
  cat "${CONSUMER_LOG}"
  kill "${CONSUMER_PID}" 2>/dev/null || true
  exit 1
fi
echo "Consumer ready."

# (c) run producer synchronously
python3 "${HOME_DIR}/load_producer.py" \
  --count "${COUNT}" \
  --run-id "${RUN_ID}" \
  --concurrency "${CONCURRENCY}" \
  --log "${INSERT_LOG}"

echo "Producer finished. Waiting for consumer to drain remaining messages..."

# (d) wait for the consumer to finish on its own
set +e
wait "${CONSUMER_PID}"
CONSUMER_EXIT=$?
set -e

echo "--- consumer log ---"
cat "${CONSUMER_LOG}"

if [ "${CONSUMER_EXIT}" -eq 2 ]; then
  echo "NOTE: consumer stopped early on its idle timeout - some messages may be missing (see stats below)."
elif [ "${CONSUMER_EXIT}" -ne 0 ]; then
  echo "WARNING: consumer exited with code ${CONSUMER_EXIT}"
fi

# (e) stats
echo
echo "=== Stats ==="
JSON_OUT="${HOME_DIR}/results_${RUN_ID}.json"
python3 "${HOME_DIR}/compute_load_stats.py" \
  --insert-log "${INSERT_LOG}" --receipt-log "${CSV}" \
  --run-id "${RUN_ID}" --json-out "${JSON_OUT}"
