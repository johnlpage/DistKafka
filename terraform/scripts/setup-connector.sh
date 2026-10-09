#!/bin/bash
set -euxo pipefail

KAFKA_DIR="/opt/kafka"
CONNECTOR_VERSION="3.1.0"
CONNECTOR_DIR="${KAFKA_DIR}/plugins/mongodb"

# Source environment variables
if [ -f /home/ec2-user/.env ]; then
  source /home/ec2-user/.env
fi

MONGO_URI="${MONGO_URI:-}"
MONGO_DB_NAME="${MONGO_DB_NAME:-bank}"
MONGO_COLLECTION="${MONGO_COLLECTION:-tasks}"
KAFKA_TOPIC="${KAFKA_TOPIC:-bank.tasks}"

if [ -z "${MONGO_URI}" ]; then
  echo "ERROR: MONGO_URI not set"
  exit 1
fi

# Wait for Kafka to be ready (uses bash /dev/tcp - `nc` isn't installed
# by default on this AMI, so `nc -z` would silently fail every iteration)
echo "Waiting for Kafka on port 9092..."
for i in $(seq 1 15); do
  if (exec 3<>/dev/tcp/localhost/9092) 2>/dev/null; then
    echo "Kafka is ready (after $((i * 2))s)"
    break
  fi
  if [ "$i" -eq 15 ]; then
    echo "Kafka did not start in time"
    exit 1
  fi
  sleep 2
done

# Create topic
${KAFKA_DIR}/bin/kafka-topics.sh --create --if-not-exists \
  --topic "${KAFKA_TOPIC}" \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 || true

# Download MongoDB Kafka Connector
mkdir -p "${CONNECTOR_DIR}"
if [ ! -f "${CONNECTOR_DIR}/mongo-kafka-connect-${CONNECTOR_VERSION}-confluent.jar" ]; then
  CONNECTOR_ZIP="/tmp/mongodb-kafka-connect.zip"
  curl -fsSL \
    "https://github.com/mongodb/mongo-kafka/releases/download/r${CONNECTOR_VERSION}/mongodb-kafka-connect-mongodb-${CONNECTOR_VERSION}.zip" \
    -o "${CONNECTOR_ZIP}"

  mkdir -p /tmp/connector-extract
  if ! command -v unzip >/dev/null 2>&1; then
    (yum install -y unzip || dnf install -y unzip)
  fi
  unzip -o "${CONNECTOR_ZIP}" -d /tmp/connector-extract
  cp /tmp/connector-extract/mongodb-kafka-connect-mongodb-${CONNECTOR_VERSION}/lib/*.jar "${CONNECTOR_DIR}/"
  rm -rf /tmp/connector-extract "${CONNECTOR_ZIP}"
fi

# Install Python dependencies (python3-pip is not installed by default on
# this AMI - only python3-pip-wheel is present, so the pip3 binary itself
# is missing until we install it explicitly)
if ! command -v pip3 >/dev/null 2>&1; then
  (dnf install -y python3-pip || yum install -y python3-pip)
fi
pip3 install --quiet pymongo kafka-python

# Install mongosh, so you can connect directly with the same combined
# multi-region connection string from .env, e.g.:
#   set -a; source .env; set +a; mongosh "$MONGO_URI"
MONGOSH_VERSION="2.12.0"
if ! command -v mongosh >/dev/null 2>&1; then
  MONGOSH_RPM="/tmp/mongodb-mongosh-${MONGOSH_VERSION}.x86_64.rpm"
  curl -fsSL -o "${MONGOSH_RPM}" \
    "https://downloads.mongodb.com/compass/mongodb-mongosh-${MONGOSH_VERSION}.x86_64.rpm"
  (dnf install -y "${MONGOSH_RPM}" || yum install -y "${MONGOSH_RPM}")
  rm -f "${MONGOSH_RPM}"
fi

# Configure connect-standalone.properties
cat > "${KAFKA_DIR}/config/connect-standalone.properties" <<CONNECTPROPS
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=/var/lib/kafka/connect.offsets
offset.flush.interval.ms=10000
plugin.path=${CONNECTOR_DIR}
CONNECTPROPS

# Configure mongodb-source.properties
# topic.namespace.map explicitly pins the MongoDB "database.collection"
# namespace to KAFKA_TOPIC. Without this, the connector's default naming
# (topic.prefix + "." + database + "." + collection, when topic.prefix is
# set) produces a *different* topic than the one this script pre-creates
# and that consumer.py subscribes to (e.g. "bank.bank.tasks" vs.
# "bank.tasks"), silently splitting producer/consumer onto two topics.
cat > "${KAFKA_DIR}/config/mongodb-source.properties" <<SOURCEPROPS
name=mongodb-source
connector.class=com.mongodb.kafka.connect.MongoSourceConnector
connection.uri=${MONGO_URI}
database=${MONGO_DB_NAME}
collection=${MONGO_COLLECTION}
topic.namespace.map={"${MONGO_DB_NAME}.${MONGO_COLLECTION}":"${KAFKA_TOPIC}"}
pipeline=[{"\$match":{"operationType":{"\$in":["insert","update","replace","delete"]}}}]
output.format.per-operation=true
change.stream.full.document=updateLookup
# Tuned for lowest latency: poll.await.time.ms's default of 5000 means
# the connector can wait up to 5s for new data before returning an empty
# batch, adding pure dead time under the kind of steady-but-modest
# throughput this load test produces - lowering it to 50 fixes that.
#
# poll.max.batch.size is deliberately left at its default (1000), NOT
# lowered to 1 - empirically, forcing batch size down to 1 makes things
# *worse* under concurrent/bursty load: the connector can then only pull
# one change-stream event per poll() cycle even when hundreds are
# already backlogged, turning it into a single-item-at-a-time queueing
# bottleneck (observed mean transit time ~250ms with batch.size=1 vs
# ~55-80ms with the default 1000, at the same poll.await.time.ms, same
# 1000-document/20-thread load). A large batch size lets it drain a
# backlog in one go; the low poll.await.time.ms keeps it from padding
# latency when there's no backlog to drain.
poll.await.time.ms=50
SOURCEPROPS

# Create systemd unit for Kafka Connect
cat > /etc/systemd/system/kafka-connect.service <<CONNECTSVC
[Unit]
Description=Kafka Connect (MongoDB Source)
After=kafka.service
Requires=kafka.service

[Service]
Type=simple
User=root
ExecStart=${KAFKA_DIR}/bin/connect-standalone.sh ${KAFKA_DIR}/config/connect-standalone.properties ${KAFKA_DIR}/config/mongodb-source.properties
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
CONNECTSVC

systemctl daemon-reload
systemctl enable kafka-connect
systemctl restart kafka-connect

# Verify connector is running
echo "Waiting for Kafka Connect to start..."
for i in $(seq 1 30); do
  STATUS=$(curl -s http://localhost:8083/connectors/mongodb-source/status 2>/dev/null || echo "")
  if echo "${STATUS}" | grep -q '"state":"RUNNING"'; then
    echo "MongoDB Source Connector is RUNNING"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "WARNING: Kafka Connect may not have started. Check logs with: journalctl -u kafka-connect"
  fi
  sleep 2
done

echo "Connector setup complete"