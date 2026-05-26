import os
import time
import json
import logging
import signal
import sys
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fraudlens.producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC             = os.getenv("KAFKA_TOPIC_RAW", "raw_transactions")
DELAY             = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.05"))
DATA_PATH         = os.getenv("DATA_PATH", "/data/raw/fraudTest.csv")
MAX_RETRIES       = 5
RETRY_BACKOFF     = 3

_running = True

def handle_signal(sig, frame):
    global _running
    log.info("Shutdown signal received — stopping producer gracefully")
    _running = False

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def build_producer() -> KafkaProducer:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d) — retrying in %ds",
                        attempt, MAX_RETRIES, RETRY_BACKOFF)
            time.sleep(RETRY_BACKOFF)
    log.error("Could not connect to Kafka after %d attempts. Exiting.", MAX_RETRIES)
    sys.exit(1)


def row_to_message(row: dict) -> dict:
    return {
        "trans_num":              row.get("trans_num"),
        "trans_date_trans_time":  str(row.get("trans_date_trans_time")),
        "unix_time":              int(row.get("unix_time", 0)),
        "amt":                    float(row.get("amt", 0.0)),
        "cc_num":                 int(row.get("cc_num", 0)),
        "merchant":               row.get("merchant"),
        "category":               row.get("category"),
        "merch_lat":              float(row.get("merch_lat", 0.0)),
        "merch_long":             float(row.get("merch_long", 0.0)),
        "lat":                    float(row.get("lat", 0.0)),
        "long":                   float(row.get("long", 0.0)),
        "city_pop":               int(row.get("city_pop", 0)),
        "is_fraud":               int(row.get("is_fraud", 0)),
        "first":                  row.get("first"),
        "last":                   row.get("last"),
        "gender":                 row.get("gender"),
        "dob":                    str(row.get("dob")),
        "job":                    row.get("job"),
        "street":                 row.get("street"),
        "city":                   row.get("city"),
        "state":                  row.get("state"),
        "zip":                    str(row.get("zip")),
        "produced_at":            datetime.now(timezone.utc).isoformat(),
    }


def on_send_error(exc):
    log.error("Failed to deliver message: %s", exc)


def main():
    log.info("Starting FraudLens Kafka producer")
    log.info("  Topic  : %s", TOPIC)
    log.info("  Broker : %s", BOOTSTRAP_SERVERS)
    log.info("  Delay  : %.3fs (%.0f msg/s)", DELAY, 1 / DELAY if DELAY > 0 else 0)
    log.info("  Source : %s", DATA_PATH)

    if not os.path.exists(DATA_PATH):
        log.error("Dataset not found: %s", DATA_PATH)
        sys.exit(1)

    producer = build_producer()

    log.info("Loading dataset...")
    df = pd.read_csv(DATA_PATH, parse_dates=["trans_date_trans_time"])
    df = df.sort_values("unix_time").reset_index(drop=True)
    total = len(df)
    log.info("Loaded %s rows — streaming in chronological order", f"{total:,}")

    sent = 0
    fraud_sent = 0
    errors = 0
    start_time = time.time()
    last_log_time = start_time

    for _, row in df.iterrows():
        if not _running:
            break

        try:
            msg = row_to_message(row.to_dict())
            key = msg["trans_num"] or str(sent)

            producer.send(
                TOPIC,
                key=key,
                value=msg,
            ).add_errback(on_send_error)

            sent += 1
            if msg["is_fraud"] == 1:
                fraud_sent += 1

            now = time.time()
            if now - last_log_time >= 10:
                elapsed = now - start_time
                rate = sent / elapsed if elapsed > 0 else 0
                pct = (sent / total) * 100
                log.info(
                    "Progress: %s/%s (%.1f%%) | %.0f msg/s | fraud: %s | errors: %s",
                    f"{sent:,}", f"{total:,}", pct, rate, f"{fraud_sent:,}", errors
                )
                last_log_time = now

            if DELAY > 0:
                time.sleep(DELAY)

        except Exception as exc:
            log.warning("Row %d skipped — %s", sent, exc)
            errors += 1

    producer.flush()
    elapsed = time.time() - start_time
    log.info("Stream complete: %s messages sent in %.1fs | fraud: %s | errors: %s",
             f"{sent:,}", elapsed, f"{fraud_sent:,}", errors)
    producer.close()


if __name__ == "__main__":
    main()
