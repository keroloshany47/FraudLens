# Spark — Stream Processing Layer

Apache Spark Structured Streaming is the processing engine of FraudLens. It consumes raw transactions from Kafka, enriches each event with computed features, scores them for fraud risk, and writes results to PostgreSQL in near-real-time.

## Why Spark Structured Streaming?

Spark treats a stream as an unbounded table. Instead of writing custom consumer loops, you write a SQL-like transformation once and Spark handles the continuous execution, checkpointing, and fault tolerance. This makes the streaming code look almost identical to a batch job — which means it is easy to test, debug, and reason about.

We chose **micro-batch mode** with a 5-second trigger interval over Spark's continuous processing mode. Continuous mode has lower latency (~1ms) but is still experimental. Micro-batch gives us exactly-once semantics through checkpointing, stable throughput, and roughly 40% lower CPU usage — an acceptable trade for 5-second alert latency in a fraud detection context.

## Architecture

```
Kafka: raw_transactions
        |
        v
Spark readStream (Kafka source)
        |
        v
Parse JSON → RAW_SCHEMA (23 fields)
        |
        v
Enrich:
  + trans_at       (parse timestamp string)
  + distance_km    (haversine customer ↔ merchant)
  + risk_score     (weighted rule engine)
  + alert_reason   (human-readable explanation)
  + hour_of_day    (time feature)
        |
        v
foreachBatch (every 5 seconds):
  ├── write ALL rows → PostgreSQL transactions table
  └── write fraud rows (is_fraud=1 OR risk_score≥0.7) → fraud_alerts table
        |
        v (on failure)
DLQ: dlq_transactions (failed rows with error reason)
```

## Files

| File | Purpose |
|---|---|
| `jobs/stream_processor.py` | Main streaming job — entry point |
| `jobs/utils/geo_utils.py` | Haversine distance calculation |
| `jobs/utils/fraud_scorer.py` | Risk scoring + alert reason generation |
| `jobs/utils/dlq_handler.py` | Routes failed batches to Kafka DLQ topic |
| `tests/test_fraud_scorer.py` | 11 unit tests — run with pytest |

## Fraud scoring logic

Each transaction receives a `risk_score` between 0.0 and 1.0 computed from three signals:

```
risk_score = (0.4 × amount_score) + (0.4 × distance_score) + (0.2 × category_score)
```

**Amount score** — large transactions carry more risk:
- > $1,000 → 1.0
- > $500 → 0.7
- > $200 → 0.4
- otherwise → 0.1

**Distance score** — transaction far from customer's home address:
- > 500km → 1.0
- > 200km → 0.7
- > 50km → 0.3
- otherwise → 0.0

**Category score** — some merchant categories have higher fraud rates:
- `shopping_net`, `misc_net`, `grocery_pos`, `shopping_pos`, `misc_pos` → 0.6
- `entertainment`, `gas_transport`, `food_dining` → 0.3
- everything else → 0.1

A fraud alert is written when `is_fraud = 1` (ground truth) **or** `risk_score >= 0.7` (our engine's independent prediction). This separation lets the OLAP layer calculate precision and recall — how well our rule engine agrees with the ground truth label.

## Running the streaming job

```bash
make spark-submit
```

This submits `stream_processor.py` to the Spark cluster with the PostgreSQL JDBC driver and Kafka connector packages. The job runs continuously until stopped.

Monitor at **http://localhost:8080** (Spark Master UI) — you will see the streaming job listed under Running Applications with throughput metrics.

## Unit tests

```bash
pytest spark/tests/ -v
```

All 11 tests cover the three utility modules independently of Spark, making them fast (no cluster needed) and reliable in CI.

## Checkpointing

Spark writes checkpoint data to `/opt/spark-apps/checkpoints/oltp_writer` inside the container. This records exactly which Kafka offsets have been processed. If the job crashes and restarts, it resumes from the last checkpoint — no messages are reprocessed, no messages are lost. This is what "exactly-once semantics" means in practice.

## Dead Letter Queue

Every `foreachBatch` call is wrapped in a try/except. If writing to PostgreSQL fails for any reason, `dlq_handler.py` catches the exception, attaches the error message and a timestamp, and publishes every failed row to the `dlq_transactions` Kafka topic. Nothing is silently lost.
