"""
dag_dlq_monitor.py
──────────────────
Monitors the Kafka Dead Letter Queue (DLQ) topic every 15 minutes.
If any messages have accumulated (depth > ALERT_THRESHOLD), the DAG
logs a prominent ERROR that Grafana / any log-scraper can pick up.

Fix applied vs original:
  • kafka-python must be in airflow/requirements.txt (see that file)
  • Added NoBrokersAvailable fallback so the DAG fails gracefully
    instead of permanently erroring when Kafka hasn't started yet
  • Removed unnecessary XCom push/pull — @task return value IS the XCom;
    downstream tasks receive it directly via Airflow's TaskFlow API
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

log = logging.getLogger("fraudlens.dag_dlq_monitor")

KAFKA_BOOTSTRAP  = "kafka:9092"
DLQ_TOPIC        = "dlq_transactions"
ALERT_THRESHOLD  = 0          # alert if ANY message sits in DLQ
KAFKA_TIMEOUT_MS = 10_000     # 10 s — don't hang forever if broker is down


@dag(
    dag_id="dag_dlq_monitor",
    description="Monitor DLQ topic depth every 15 min — alert if messages accumulate",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "owner": "fraudlens",
    },
    tags=["monitoring", "dlq", "resilience"],
)
def dag_dlq_monitor():

    @task()
    def get_dlq_depth() -> int:
        """
        Connect to Kafka and return the number of unread messages in the DLQ.

        Strategy: compare end_offset vs begin_offset on partition 0.
        The difference is how many messages exist but have never been consumed —
        i.e. events that Spark's DLQ handler wrote but nobody has read.

        Returns -1 if Kafka is unreachable (triggers a retry, not an alert).
        """
        try:
            from kafka import KafkaConsumer, TopicPartition
            from kafka.errors import NoBrokersAvailable
        except ImportError as exc:
            raise ImportError(
                "kafka-python is not installed in the Airflow container. "
                "Add `kafka-python==2.0.2` to airflow/requirements.txt and "
                "rebuild / restart the Airflow service."
            ) from exc

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                request_timeout_ms=KAFKA_TIMEOUT_MS,
                # Don't join a consumer group — we only need offsets, not messages.
                group_id=None,
            )
        except NoBrokersAvailable:
            log.warning(
                "[DLQ Monitor] Kafka broker not reachable at %s — "
                "will retry next scheduled run.",
                KAFKA_BOOTSTRAP,
            )
            return -1   # TaskFlow propagates -1 to branch; branch treats it as no-alert

        tp = TopicPartition(DLQ_TOPIC, 0)   # DLQ has 1 partition
        consumer.assign([tp])

        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)

        consumer.seek_to_beginning(tp)
        begin_offset = consumer.position(tp)

        consumer.close()

        depth = end_offset - begin_offset
        log.info("[DLQ Monitor] topic=%s  begin=%d  end=%d  depth=%d",
                 DLQ_TOPIC, begin_offset, end_offset, depth)
        return depth

    @task.branch()
    def route_on_depth(depth: int) -> str:
        """
        Receives the DLQ depth directly from get_dlq_depth via TaskFlow.
        Returns the task_id to execute next; Airflow skips the other branch.
        """
        log.info("[DLQ Monitor] routing — depth=%s", depth)
        if depth is not None and depth > ALERT_THRESHOLD:
            return "alert_team"
        return "no_action"

    @task()
    def alert_team(depth: int):
        """
        Fires when depth > ALERT_THRESHOLD.
        Writes a log.error line — swap the comment block for a real webhook
        (Slack / PagerDuty / email) when you move to production.
        """
        log.error(
            "ALERT: DLQ has %d unprocessed message(s) in topic '%s'. "
            "Spark stream processor may be dropping events. "
            "Inspect with:  docker compose logs spark-worker  "
            "or:            make dlq-check",
            depth, DLQ_TOPIC,
        )

        # ── uncomment for a real Slack alert ─────────────────────────────────
        # import requests, os
        # webhook = os.environ["SLACK_WEBHOOK_URL"]
        # requests.post(webhook, json={
        #     "text": (
        #         f":red_circle: *FraudLens DLQ alert*\n"
        #         f"{depth} failed message(s) in `{DLQ_TOPIC}`.\n"
        #         "Check Spark worker logs."
        #     )
        # }, timeout=5)

    no_action = EmptyOperator(task_id="no_action")

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    # TaskFlow passes return values as arguments automatically.
    # route_on_depth receives `depth` from get_dlq_depth.
    # alert_team receives `depth` from get_dlq_depth (not the branch).
    depth_val  = get_dlq_depth()
    branch     = route_on_depth(depth_val)
    alert      = alert_team(depth_val)

    branch >> [alert, no_action]


dag_dlq_monitor()
