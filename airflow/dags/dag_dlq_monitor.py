import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger("fraudlens.dag_dlq_monitor")

KAFKA_BOOTSTRAP = "kafka:9092"
DLQ_TOPIC       = "dlq_transactions"
ALERT_THRESHOLD = 0      # alert if ANY message lands in DLQ
XCOM_KEY        = "dlq_depth"


@dag(
    dag_id="dag_dlq_monitor",
    description="Monitor DLQ topic depth every 15 min — alert if messages accumulate",
    schedule="*/15 * * * *",         # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "owner": "fraudlens",
    },
    tags=["monitoring", "dlq", "resilience"],
)
def dag_dlq_monitor():

    @task()
    def get_dlq_depth(**context) -> int:
        """
        Connect to Kafka and measure how many unread messages
        are sitting in the DLQ topic.

        We use the TopicPartition API to compare the end offset
        (latest message position) with the beginning offset
        (oldest message position).  The difference is the depth —
        how many messages have never been consumed.

        The result is pushed to XCom so the next task can read it.
        XCom (cross-communication) is Airflow's built-in key-value
        store for passing small data between tasks in the same DAG run.
        """
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
        tp = TopicPartition(DLQ_TOPIC, 0)   # partition 0 — DLQ has only 1
        consumer.assign([tp])

        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)

        consumer.seek_to_beginning(tp)
        begin_offset = consumer.position(tp)

        consumer.close()

        depth = end_offset - begin_offset
        log.info("[DLQ Monitor] topic=%s depth=%d", DLQ_TOPIC, depth)

        # push to XCom — task_instance is injected by Airflow via **context
        context["task_instance"].xcom_push(key=XCOM_KEY, value=depth)
        return depth

    @task.branch()
    def route_on_depth(**context) -> str:
        """
        BranchPythonOperator task — reads the DLQ depth from XCom
        and returns the task_id of the next task to execute.
        Airflow skips all other downstream branches automatically.
        """
        depth = context["task_instance"].xcom_pull(
            task_ids="get_dlq_depth",
            key=XCOM_KEY,
        )
        log.info("[DLQ Monitor] routing — depth=%s", depth)

        if depth is not None and depth > ALERT_THRESHOLD:
            return "alert_team"
        return "no_action"

    @task()
    def alert_team(**context):
        """
        In production this would send a Slack message or PagerDuty alert.
        For portfolio: writes a prominent log entry that Grafana can pick up,
        and prints the DLQ message count.
        Replace the log.error with an HTTP call to your alerting system.
        """
        depth = context["task_instance"].xcom_pull(
            task_ids="get_dlq_depth",
            key=XCOM_KEY,
        )
        log.error(
            "ALERT: DLQ has %d unprocessed messages in topic '%s'. "
            "Spark stream processor may be failing. "
            "Check logs: make logs-spark",
            depth, DLQ_TOPIC,
        )
        # ── swap this section for a real alert in production ──
        # import requests
        # requests.post(SLACK_WEBHOOK_URL, json={
        #     "text": f":red_circle: FraudLens DLQ alert: {depth} failed messages"
        # })

    no_action = EmptyOperator(task_id="no_action")

    # ── DEPENDENCY CHAIN ──────────────────────────────────────
    depth   = get_dlq_depth()
    branch  = route_on_depth()
    alert   = alert_team()

    depth >> branch >> [alert, no_action]


dag_dlq_monitor()
