"""
fraudlens_exporter.py
─────────────────────
Custom Prometheus exporter for FraudLens.

Exposes two metrics on :8000/metrics:

  fraudlens_dlq_depth
      Number of unconsumed messages in the dlq_transactions Kafka topic.
      Gauge — goes up when Spark drops events, should return to 0 when replayed.

  fraudlens_events_per_second
      Rate of transaction events arriving in transactions_topic, measured
      over the last scrape interval. This is what the Grafana DLQ panel
      and pipeline health panel need.

Run inside Docker — the exporter container lives on the same network as Kafka
and PostgreSQL, so it can reach both by service name.
"""

import os
import time
import logging
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
from prometheus_client import Gauge, start_http_server

log = logging.getLogger("fraudlens_exporter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Config (overridable via env vars) ──────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq_transactions")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "raw_transactions")
SCRAPE_INTERVAL_SECS = int(os.getenv("SCRAPE_INTERVAL", "15"))
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8000"))

# ── Metrics ────────────────────────────────────────────────────────────────────
DLQ_DEPTH = Gauge(
    "fraudlens_dlq_depth",
    "Number of unconsumed messages in the FraudLens Dead Letter Queue topic",
)

EVENTS_PER_SECOND = Gauge(
    "fraudlens_events_per_second",
    "Rate of transaction events arriving in the main Kafka topic (msgs/sec)",
)

# ── Helpers ────────────────────────────────────────────────────────────────────


def _get_end_offset(consumer: KafkaConsumer, topic: str) -> int:
    """Return the latest (end) offset for partition 0 of a topic."""
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    return consumer.position(tp)


def _get_begin_offset(consumer: KafkaConsumer, topic: str) -> int:
    """Return the earliest (begin) offset for partition 0 of a topic."""
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    return consumer.position(tp)


def collect_metrics(prev_txn_offset: int, prev_time: float) -> tuple[int, float]:
    """
    Query Kafka for current offsets and update Prometheus gauges.

    Returns (current_txn_end_offset, current_time) so the caller
    can compute the rate on the next iteration.
    """
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=8_000,
            group_id=None,  # observer only — don't commit offsets
        )
    except NoBrokersAvailable:
        log.warning(
            "Kafka broker not reachable at %s — skipping scrape", KAFKA_BOOTSTRAP
        )
        return prev_txn_offset, prev_time

    try:
        # ── DLQ depth ────────────────────────────────────────────────────────
        dlq_end = _get_end_offset(consumer, DLQ_TOPIC)
        dlq_begin = _get_begin_offset(consumer, DLQ_TOPIC)
        depth = dlq_end - dlq_begin
        DLQ_DEPTH.set(depth)
        log.info("dlq_depth=%d (begin=%d end=%d)", depth, dlq_begin, dlq_end)

        # ── Events per second ─────────────────────────────────────────────────
        now = time.monotonic()
        txn_end = _get_end_offset(consumer, TRANSACTIONS_TOPIC)
        elapsed = now - prev_time
        delta_msgs = txn_end - prev_txn_offset

        if elapsed > 0 and prev_txn_offset >= 0:
            rate = delta_msgs / elapsed
            EVENTS_PER_SECOND.set(rate)
            log.info(
                "events_per_second=%.2f (delta=%d msgs in %.1fs)",
                rate,
                delta_msgs,
                elapsed,
            )
        else:
            EVENTS_PER_SECOND.set(0)

        return txn_end, now

    finally:
        consumer.close()


# ── Main loop ──────────────────────────────────────────────────────────────────


def main():
    log.info("Starting FraudLens Prometheus exporter on :%d", EXPORTER_PORT)
    log.info(
        "Kafka: %s | DLQ topic: %s | txn topic: %s",
        KAFKA_BOOTSTRAP,
        DLQ_TOPIC,
        TRANSACTIONS_TOPIC,
    )

    start_http_server(EXPORTER_PORT)
    log.info("Metrics available at http://localhost:%d/metrics", EXPORTER_PORT)

    prev_txn_offset = -1  # -1 signals "no previous reading yet"
    prev_time = time.monotonic()

    while True:
        prev_txn_offset, prev_time = collect_metrics(prev_txn_offset, prev_time)
        time.sleep(SCRAPE_INTERVAL_SECS)


if __name__ == "__main__":
    main()
