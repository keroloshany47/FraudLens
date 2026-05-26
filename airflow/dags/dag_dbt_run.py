import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

log = logging.getLogger("fraudlens.dag_dbt_run")

DBT_DIR         = "/opt/airflow/dbt"
DBT_PROFILES    = "/opt/airflow/dbt"
DBT_TARGET      = "dev"


def run_dbt(command: str) -> None:
    """
    Execute a dbt CLI command inside the Airflow worker.
    Raises AirflowException if the command exits with a non-zero code
    so Airflow marks the task as failed and retries it.
    """
    import subprocess
    full_cmd = (
        f"cd {DBT_DIR} && "
        f"dbt {command} --profiles-dir {DBT_PROFILES} --target {DBT_TARGET}"
    )
    log.info("Running: %s", full_cmd)
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)

    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.warning(result.stderr)

    if result.returncode != 0:
        raise AirflowException(
            f"dbt command failed (exit {result.returncode}): {command}\n"
            f"{result.stderr}"
        )


@dag(
    dag_id="dag_dbt_run",
    description="Daily dbt run: test staging → build all models → test marts",
    schedule="0 6 * * *",            # every day at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "fraudlens",
    },
    tags=["dbt", "olap", "transform"],
)
def dag_dbt_run():

    @task()
    def test_staging():
        """
        Run dbt tests on staging models BEFORE building anything.
        If source data is broken (nulls in trans_id, negative amounts),
        we stop here — never build a mart on top of bad data.
        """
        run_dbt("test --select staging")

    @task()
    def run_staging():
        """
        Build all stg_* models.
        These clean and type-cast the raw OLTP tables:
        - stg_transactions: casts types, adds source filter
        - stg_customers:    deduplicates, trims whitespace
        - stg_merchants:    deduplicates, normalises category names
        """
        run_dbt("run --select staging")

    @task()
    def run_intermediate():
        """
        Build intermediate models that join staging tables together.
        int_transaction_stats enriches each transaction with:
        - customer age (derived from dob)
        - merchant category
        - customer state
        These columns are needed by both mart models.
        """
        run_dbt("run --select intermediate")

    @task()
    def run_marts():
        """
        Build the final analytical models that Grafana queries:
        - mart_fraud_summary:  daily fraud rate by category and state
        - mart_customer_360:   per-customer risk profile
        - fact_transactions:   central star-schema fact table
        - dim_customer:        customer dimension
        - dim_merchant:        merchant dimension
        - dim_date:            date dimension
        """
        run_dbt("run --select marts")

    @task()
    def test_all():
        """
        Run ALL dbt tests after the full build.
        This catches issues that only appear after models are joined
        together — referential integrity, uniqueness in fact table,
        valid ranges in mart aggregations.
        """
        run_dbt("test")

    @task()
    def generate_docs():
        """
        Regenerate the dbt data catalog (manifest.json + catalog.json).
        The CD workflow picks these up and publishes to GitHub Pages.
        """
        run_dbt("docs generate")
        log.info("dbt docs regenerated — catalog is up to date")

    # ── DEPENDENCY CHAIN ──────────────────────────────────────
    # strict left-to-right: each task waits for the previous to succeed
    (
        test_staging()
        >> run_staging()
        >> run_intermediate()
        >> run_marts()
        >> test_all()
        >> generate_docs()
    )


dag_dbt_run()
