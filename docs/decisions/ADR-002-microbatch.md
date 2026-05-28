# ADR-002: Micro-batch over continuous streaming

**Status:** Accepted | **Date:** 2024-05

## Context
Spark Structured Streaming offers two execution modes:
1. Continuous processing — ~1ms latency, experimental
2. Micro-batch — configurable trigger interval, stable

## Decision
Use micro-batch with a 5-second trigger interval.

## Reasons
- Continuous mode is still experimental in Spark 3.5 and not recommended for production
- 5-second latency is acceptable for fraud detection — we are detecting fraud for analyst review, not blocking a payment in real time
- Micro-batch gives exactly-once semantics through Spark checkpointing — if the job crashes and restarts, no messages are reprocessed or lost
- Approximately 40% lower CPU usage vs continuous mode at equivalent throughput
- Each micro-batch is an atomic transaction — simpler to reason about and debug

## Trade-offs accepted
- 5-second alert latency (acceptable for fraud analytics, not for real-time payment authorization)
- Batch boundary edge cases can affect time-window aggregations near the trigger interval
