# Kafka — Message Broker Layer

Apache Kafka is the backbone of FraudLens's real-time path. Every transaction from the streaming dataset passes through Kafka before reaching Spark.

## Why Kafka?

Kafka is a distributed, log-based message broker. Unlike a queue (RabbitMQ, SQS) that deletes a message after it is consumed, Kafka stores messages on disk for a configurable retention period. This gives us two things a financial pipeline needs: **replay** (reprocess failed batches without data loss) and **audit trail** (every message that ever entered the system is preserved).

We use **KRaft mode** — Kafka without Zookeeper. Since Kafka 3.3, KRaft is production-stable and removes the operational overhead of running a separate Zookeeper cluster alongside Kafka. One fewer service, same guarantees.

## Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `raw_transactions` | 3 | Raw JSON events from the Python producer |
| `enriched_transactions` | 3 | Spark-enriched events with risk score and distance |
| `fraud_alerts` | 1 | Confirmed fraud events for Grafana real-time dashboard |
| `dlq_transactions` | 1 | Failed messages — written here instead of being lost |

Three partitions on the ingestion topics allow Spark to parallelize consumption across its workers. One partition on `fraud_alerts` and `dlq_transactions` is intentional — ordering matters for alerts and failure records.

## Producer

The producer lives in `producer/stream_producer.py`. It reads `fraudTest.csv` row by row and publishes each transaction as a JSON message to `raw_transactions`. Key design decisions:

- **`acks="all"`** — the broker only confirms delivery after all in-sync replicas have written the message. This prevents data loss if the broker crashes mid-write.
- **`max_in_flight_requests_per_connection=1`** — combined with retries, this guarantees message ordering within a partition. Without it, a retry could overtake a successful send.
- **`compression_type="gzip"`** — reduces network and disk usage by roughly 60% for JSON payloads.
- **Partition key = `trans_num`** — all events for the same transaction always land in the same partition, preserving causal order.
- **`produced_at` timestamp** — added by the producer so Spark can calculate end-to-end pipeline latency.

## Message format

Every message is a JSON object with 23 original Sparkov fields plus `produced_at`:

```json
{
  "trans_num": "3d3b6e4e...",
  "trans_date_trans_time": "2020-06-21 12:14:25",
  "amt": 14.99,
  "cc_num": 4532939809883,
  "merchant": "fraud_Kirlin and Sons",
  "category": "grocery_pos",
  "merch_lat": 36.011,
  "merch_long": -81.933,
  "lat": 36.127,
  "long": -81.997,
  "is_fraud": 0,
  "produced_at": "2024-05-24T11:04:33Z"
}
```

## Setup

Topics are created by running:

```bash
make kafka-topics
```

This calls `topics/create_topics.sh` which uses the Kafka CLI inside the container. Safe to re-run — `--if-not-exists` prevents errors.

## Useful commands

```bash
make kafka-status        # list all topics with partition info
make kafka-lag           # show consumer group lag
make dlq-check           # inspect dead letter queue messages
make stream              # start the producer
make stream-stop         # stop the producer
```

## Kafka UI

Browse topics, messages, and consumer lag visually at **http://localhost:8090**. No credentials needed. The `raw_transactions` topic shows live messages as the producer runs.

## Dead Letter Queue

When Spark fails to process a batch (schema mismatch, database unreachable, bad data), the failed rows are routed to `dlq_transactions` instead of being silently dropped. The Airflow DAG `dag_dlq_monitor` checks this topic every 15 minutes and alerts if the message count grows. This is the difference between a production pipeline and a tutorial pipeline.
