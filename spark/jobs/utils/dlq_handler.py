import json
import logging
from datetime import datetime, timezone


log = logging.getLogger("fraudlens.dlq")

_producer = None


def get_producer(bootstrap_servers: str = "kafka:9092"):
    global _producer
    if _producer is None:
        from kafka import KafkaProducer

        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
    return _producer


def send_to_dlq(rows: list, error_reason: str, topic: str = "dlq_transactions"):
    try:
        producer = get_producer()
        for row in rows:
            msg = dict(row)
            msg["_dlq_reason"] = error_reason
            msg["_dlq_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(topic, value=msg)
        producer.flush()
        log.warning("[DLQ] Routed %d rows — reason: %s", len(rows), error_reason)
    except Exception as exc:
        log.error("[DLQ] Failed to route to DLQ: %s", exc)
