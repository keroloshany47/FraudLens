import json
import logging
import os
import signal
import sys
import time
import hashlib
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fraudlens.producer")

# ---------------- CONFIG ----------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw_transactions")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dlq_transactions")
DATA_PATH = os.getenv("DATA_PATH", "/data/raw/fraudTest.csv")
DELAY = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.05"))

MAX_RETRIES = 5
RETRY_BACKOFF = 3

_running = True


# ---------------- SIGNAL HANDLING ----------------
def handle_signal(sig, frame):
    global _running
    log.info("Shutdown signal received — stopping producer")
    _running = False


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


# ---------------- SURROGATE KEY GENERATION ----------------
def hash_id(value: str) -> int:
    """Deterministic surrogate key generator"""
    return int(hashlib.md5(str(value).encode()).hexdigest()[:12], 16)


# ---------------- PRODUCER ----------------
def build_producer() -> KafkaProducer:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=10,
                compression_type="gzip",
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %s/%s)", attempt, MAX_RETRIES)
            time.sleep(RETRY_BACKOFF)

    log.error("Kafka unavailable — exiting")
    sys.exit(1)


# ---------------- VALIDATION ----------------
def validate_row(row: dict) -> bool:
    """Hard validation before sending"""
    required_fields = ["trans_num", "cc_num", "merchant"]

    for f in required_fields:
        if not row.get(f):
            return False

    return True


# ---------------- MESSAGE BUILDER ----------------
def row_to_message(row: dict) -> dict:
    """
    FIXED VERSION:
    - ensures customer_id exists
    - ensures merchant_id exists
    - never sends NULL FK
    """

    customer_id = row.get("cc_num")
    merchant_raw = row.get("merchant")

    return {
        "trans_id": row.get("trans_num"),

        # FIX: surrogate keys instead of missing IDs
        "customer_id": hash_id(customer_id) if customer_id else -1,
        "merchant_id": hash_id(merchant_raw) if merchant_raw else -1,

        "trans_date": str(row.get("trans_date_trans_time")),
        "amount": float(row.get("amt", 0.0)),

        "lat": float(row.get("lat", 0.0)),
        "long": float(row.get("long", 0.0)),

        "category": row.get("category"),
        "is_fraud": int(row.get("is_fraud", 0)),

        "source": "stream",

        "produced_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------- DLQ HANDLER ----------------
def send_dlq(producer, row, reason):
    msg = {
        "reason": reason,
        "raw": row,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    producer.send(DLQ_TOPIC, value=msg)


# ---------------- MAIN ----------------
def main():
    log.info("Starting FraudLens Producer (FIXED VERSION)")
    log.info("Topic: %s", TOPIC)

    if not os.path.exists(DATA_PATH):
        log.error("Dataset not found: %s", DATA_PATH)
        sys.exit(1)

    producer = build_producer()

    df = pd.read_csv(DATA_PATH)
    df = df.sort_values("unix_time").reset_index(drop=True)

    total = len(df)
    sent = 0
    dropped = 0

    log.info("Loaded %s rows", f"{total:,}")

    for _, row in df.iterrows():
        if not _running:
            break

        row_dict = row.to_dict()

        try:
            # ---------------- VALIDATION ----------------
            if not validate_row(row_dict):
                send_dlq(producer, row_dict, "missing_required_fields")
                dropped += 1
                continue

            msg = row_to_message(row_dict)
            key = msg["trans_id"]

            producer.send(
                TOPIC,
                key=key,
                value=msg
            )

            sent += 1

            if DELAY > 0:
                time.sleep(DELAY)

        except Exception as e:
            log.error("Failed row → DLQ: %s", e)
            send_dlq(producer, row_dict, str(e))
            dropped += 1

    producer.flush()
    producer.close()

    log.info(
        "DONE → sent=%s dropped=%s",
        f"{sent:,}",
        f"{dropped:,}"
    )


if __name__ == "__main__":
    main()