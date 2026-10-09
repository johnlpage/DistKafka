#!/bin/bash
set -euxo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
SCALA_VERSION="2.13"
KAFKA_DIR="/opt/kafka"
KAFKA_DATA="/var/lib/kafka/data"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

# Determine private IP for advertised listeners (AL2023 requires IMDSv2 token)
IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null || echo "")
if [ -n "${IMDS_TOKEN}" ]; then
  PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" http://169.254.169.254/latest/meta-data/local-ipv4)
else
  PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
fi

if ! java -version &>/dev/null; then
  echo "Java not found - should have been installed via user_data"
  exit 1
fi

# Download and extract Kafka. Plain curl is sufficient: this is a *currently
# supported* release, so it resolves via Apache's fast mirror network
# (dlcdn.apache.org / downloads.apache.org) rather than the intentionally
# throttled archive.apache.org tier used for old/EOL releases.
if [ ! -d "${KAFKA_DIR}" ]; then
  mkdir -p /tmp/kafka-download
  ARCHIVE="/tmp/kafka-download/${KAFKA_TGZ}"

  curl -fL --retry 3 --retry-delay 3 --connect-timeout 15 \
    "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}" \
    -o "${ARCHIVE}" \
  || curl -fL --retry 3 --retry-delay 3 --connect-timeout 15 \
    "https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}" \
    -o "${ARCHIVE}"

  tar -xzf "${ARCHIVE}" -C /tmp/kafka-download
  mv "/tmp/kafka-download/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${KAFKA_DIR}"
  rm -rf /tmp/kafka-download
fi

mkdir -p "${KAFKA_DATA}"

# Kafka 4.x is KRaft-only (ZooKeeper was removed). Configure a single-node
# combined broker+controller.
cat > "${KAFKA_DIR}/config/server.properties" <<SERVERPROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://${PRIVATE_IP}:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${KAFKA_DATA}
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
auto.create.topics.enable=true
delete.topic.enable=true
SERVERPROPS

# Format storage on first run only (KRaft requires a cluster ID + meta.properties).
if [ ! -f "${KAFKA_DATA}/meta.properties" ]; then
  CLUSTER_ID=$("${KAFKA_DIR}/bin/kafka-storage.sh" random-uuid)
  "${KAFKA_DIR}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${KAFKA_DIR}/config/server.properties"
fi

# systemd unit for Kafka (KRaft — no ZooKeeper service needed)
cat > /etc/systemd/system/kafka.service <<KAFKASVC
[Unit]
Description=Apache Kafka (KRaft)
After=network.target

[Service]
Type=simple
User=root
ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/server.properties
ExecStop=${KAFKA_DIR}/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
KAFKASVC

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

# Wait for Kafka to be ready. Uses bash's /dev/tcp instead of `nc`, which
# is not installed by default on this AMI (nc -z was silently failing every
# time regardless of Kafka's actual state, burning the whole timeout).
# A cold KRaft single-broker start normally completes in ~5s; 15 * 2s = 30s
# gives headroom without masking a real failure for minutes.
echo "Waiting for Kafka to start on port 9092..."
KAFKA_READY=0
for i in $(seq 1 15); do
  if (exec 3<>/dev/tcp/localhost/9092) 2>/dev/null; then
    echo "Kafka is ready (after $((i * 2))s)"
    KAFKA_READY=1
    break
  fi
  sleep 2
done

if [ "${KAFKA_READY}" -ne 1 ]; then
  echo "ERROR: Kafka did not start listening on 9092 within 30s. Dumping diagnostics:"
  systemctl status kafka --no-pager || true
  journalctl -u kafka -n 100 --no-pager || true
  exit 1
fi

# Create topic early
"${KAFKA_DIR}/bin/kafka-topics.sh" --create --if-not-exists \
  --topic "${KAFKA_TOPIC:-bank.tasks}" \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 || true

echo "Kafka setup complete"