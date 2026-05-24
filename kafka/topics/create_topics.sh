#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# FraudLens — Kafka Topics Setup
# Creates all 4 topics including the Dead Letter Queue
# Safe to re-run: --if-not-exists prevents errors
# ─────────────────────────────────────────────────────────────────

set -e

KAFKA_CONTAINER="fraudlens-kafka"
BOOTSTRAP="localhost:9092"

create_topic() {
 local name=$1
 local partitions=$2
 local description=$3

 echo " Creating topic: $name (partitions: $partitions) — $description"
 docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server $BOOTSTRAP \
 --create \
 --topic "$name" \
 --partitions "$partitions" \
 --replication-factor 1 \
 --if-not-exists
}

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " FraudLens — Creating Kafka Topics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Core pipeline topics
create_topic "raw_transactions" 3 "Raw events from Python producer"
create_topic "enriched_transactions" 3 "Spark-enriched events (risk score, distance)"
create_topic "fraud_alerts" 1 "Confirmed fraud events for real-time dashboards"

# Level 4 — Dead Letter Queue
create_topic "dlq_transactions" 1 "Failed processing events — monitored by Airflow"

echo ""
echo " All topics created. Listing:"
echo ""
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server $BOOTSTRAP \
 --list
echo ""
