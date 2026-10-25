#!/bin/bash
# Creates the two topics and the ACLs that protect secure-topic.
set -euo pipefail
BIN=/opt/kafka/bin
PUBLIC=kafka:9092
SECURE=kafka:9094

cat > /tmp/admin.properties <<EOP
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOP

# public-topic: no ACLs -> anyone can read/write
$BIN/kafka-topics.sh --bootstrap-server $PUBLIC --create --if-not-exists --topic public-topic --partitions 3 --replication-factor 1
# secure-topic: ACLs below -> only listed users can access
$BIN/kafka-topics.sh --bootstrap-server $PUBLIC --create --if-not-exists --topic secure-topic --partitions 3 --replication-factor 1

# Only groups prefixed "secure-" are restricted; other groups stay open for the public topic.
# app: read + write; reader: read only. Everyone else (incl. anonymous) is denied.
$BIN/kafka-acls.sh --bootstrap-server $SECURE --command-config /tmp/admin.properties --add \
  --allow-principal User:app --operation Read --operation Write --operation Describe --topic secure-topic
$BIN/kafka-acls.sh --bootstrap-server $SECURE --command-config /tmp/admin.properties --add \
  --allow-principal User:reader --operation Read --operation Describe --topic secure-topic
$BIN/kafka-acls.sh --bootstrap-server $SECURE --command-config /tmp/admin.properties --add \
  --allow-principal User:app --allow-principal User:reader --operation Read --group secure- --resource-pattern-type prefixed

echo "Topics and ACLs ready"
