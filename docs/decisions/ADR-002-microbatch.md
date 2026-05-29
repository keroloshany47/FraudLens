# ADR-002 — Spark Structured Streaming with 5-Second Micro-Batch

## Context

FraudLens must enrich every incoming transaction with two computed fields
before writing it to PostgreSQL:

1. **`distance_km`** — Haversine distance between the cardholder's registered
   home location and the merchant's coordinates
2. **`risk_score`** — a weighted rule engine combining amount, distance, and
   merchant category signals:
   ```
   risk_score = (0.4 × amount_score) + (0.4 × distance_score) + (0.2 × category_score)
   ```

Enriched records must then be written to three PostgreSQL tables
(`transactions`, `fraud_alerts`, `customers`) and — on write failure — routed
to `dlq_transactions` on Kafka.

The key design questions were:

1. **Which Spark API** — Structured Streaming (`foreachBatch`) vs. plain
   Spark batch jobs vs. a custom consumer loop?
2. **What trigger interval** — continuous, 1 s, 5 s, 30 s, once?
3. **How to handle write failures** without silently dropping events?

---

## Decision

**Use Spark Structured Streaming with a 5-second `ProcessingTime` trigger
and `foreachBatch` sink. Failed batches are routed to a Dead Letter Queue
rather than retried inline.**

---

## Rationale

### 1. Structured Streaming over plain batch or consumer loops

Spark Structured Streaming provides:

- **Kafka offset management** via checkpointing — if the job crashes, it
  resumes from the last committed offset without reprocessing or losing events
- **Exactly-once semantics at the source** — offsets are committed only after
  `foreachBatch` completes successfully
- **Schema enforcement** — the Kafka `value` binary is deserialized against a
  declared schema, rejecting malformed events at the framework level
- A **declarative, testable computation graph** — `fraud_scorer.py` and
  `geo_utils.py` operate on plain Spark DataFrames and are fully unit-testable
  without a live Kafka cluster (11 pytest unit tests cover them)

A plain Python consumer loop would require reimplementing all of the above
manually. A Spark batch job would require an external scheduler and could not
maintain offset state between runs cleanly.

### 2. The 5-second trigger interval

| Interval | Throughput impact | Latency | DB write frequency | Fit |
|---|---|---|---|---|
| Continuous | Highest | Near-zero | Very high — connection pressure | ✗ |
| 1 s | High | ~1 s | High — 60 writes/min | ✗ |
| **5 s** | **Moderate** | **~5 s** | **Manageable — 12 writes/min** | **✓** |
| 30 s | Low | ~30 s | Low but batches grow large | ✗ |
| Once | Batch-only | N/A | N/A | ✗ |

**5 seconds** satisfies the stated SLA of sub-5-second fraud detection while
keeping PostgreSQL write pressure within the single-machine Docker Compose
budget. The producer emits one event every 0.05 s (20 events/s), so each
micro-batch contains roughly 100 events — a comfortable `execute_values`
bulk insert.

### 3. `foreachBatch` over native sinks

`foreachBatch` gives full control of the write logic:

- A single batch can write to **multiple tables** (`transactions`,
  `fraud_alerts`) in one database round-trip sequence
- **Try/except wrapping** per batch enables DLQ routing on partial failure
  without losing successfully written rows from the same batch
- **`ON CONFLICT DO NOTHING`** idempotency is easy to inject into the
  `execute_values` call — safe to reprocess if the checkpoint is reset

The native PostgreSQL sink (JDBC) writes each micro-batch atomically to one
table and provides no built-in DLQ routing.

### 4. Dead Letter Queue on batch failure

Every `foreachBatch` call wraps the PostgreSQL write in a `try/except`.
On any exception:

- The failed rows are serialized back to JSON
- Published to `dlq_transactions` with an attached `error_reason` field
- The micro-batch is marked complete (offset committed) — Spark does not
  retry the same batch

This is a deliberate **at-least-once with observable failure** model.
Silent drops are worse than observable DLQ accumulation that triggers
an Airflow alert.

---

## Alternatives Considered

| Option | Reason Rejected |
|---|---|
| **Kafka Streams** | JVM-native, tighter Kafka integration, but no native Python API; would require a separate JVM service |
| **Flink** | More powerful windowing, lower latency — but adds significant operational complexity for a single-machine portfolio project |
| **Spark Continuous Processing** | True sub-second latency but experimental as of Spark 3.5; no `foreachBatch` support |
| **Python `confluent-kafka` consumer loop** | Simple to write, but offset management, checkpointing, and schema validation must all be reimplemented manually |
| **1-second trigger** | Feasible, but 60 PostgreSQL write cycles/minute is unnecessarily aggressive given the 0.05 s/event producer rate |

---

## Consequences

**Positive:**
- Sub-5-second end-to-end latency from Kafka produce to `fraud_alerts` insert
- Resilient to job crash — Spark resumes from checkpoint without event loss
- Full unit test coverage of enrichment logic independent of Kafka/Postgres
- DLQ ensures no silent data loss

**Negative / Risks:**
- 5-second batches mean a maximum theoretical alert latency of ~5 s after
  the transaction arrives in Kafka (acceptable per project SLA)
- `foreachBatch` offset commits happen after the write, not atomically with
  it — a crash between write-success and offset-commit could produce a
  duplicate on restart. Mitigated by `ON CONFLICT DO NOTHING` idempotency

---

## Implementation Notes

```python
# spark/jobs/stream_processor.py

query = (
    enriched_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

def process_batch(batch_df, batch_id):
    try:
        write_transactions(batch_df)
        write_fraud_alerts(batch_df)
    except Exception as e:
        dlq_handler.publish(batch_df, error_reason=str(e))
```

```python
# risk_score formula — spark/jobs/utils/fraud_scorer.py
risk_score = (0.4 * amount_score) + (0.4 * distance_score) + (0.2 * category_score)
```

Checkpoint location is a Docker volume mount — survives container restarts.

---

*See also: [ADR-001](ADR-001-kafka-kraft.md) — Kafka KRaft mode,
[ADR-003](ADR-003-postgres-olap.md) — PostgreSQL as OLAP store*
