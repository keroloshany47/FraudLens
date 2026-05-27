import logging
import subprocess
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

log = logging.getLogger("fraudlens.dag_dbt_run")

DBT_DIR      = "/opt/airflow/dbt"
DBT_PROFILES = "/opt/airflow/dbt"
DBT_TARGET   = "dev"


def find_dbt() -> str:
    """Find dbt binary regardless of install location."""
    import shutil
    dbt = shutil.which("dbt")
    if dbt:
        return dbt
    # common pip --user install path
    candidates = [
        "/home/airflow/.local/bin/dbt",
        "/usr/local/bin/dbt",
        f"{sys.prefix}/bin/dbt",
    ]
    for c in candidates:
        import os
        if os.path.isfile(c):
            return c
    raise AirflowException("dbt binary not found — is dbt-postgres installed?")


def run_dbt(command: str) -> None:
    dbt_bin = find_dbt()
    full_cmd = (
        f"cd {DBT_DIR} && "
        f"{dbt_bin} {command} "
        f"--profiles-dir {DBT_PROFILES} --target {DBT_TARGET}"
    )
    log.info("Running: %s", full_cmd)
    result = subprocess.run(
        full_cmd, shell=True, capture_output=True, text=True
    )
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
    schedule="0 6 * * *",
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
    def install_deps():
        """Ensure dbt packages are installed before running."""
        run_dbt("deps")

    @task()
    def test_staging():
        run_dbt("test --select staging")

    @task()
    def run_staging():
        run_dbt("run --select staging")

    @task()
    def run_intermediate():
        run_dbt("run --select intermediate")

    @task()
    def run_marts():
        run_dbt("run --select marts")

    @task()
    def test_all():
        run_dbt("test")

    @task()
    def generate_docs():
        run_dbt("docs generate")
        log.info("dbt docs regenerated")

    install_deps() >> run_staging() >> test_staging() >> run_intermediate() >> run_marts() >> test_all() >> generate_docs()


dag_dbt_run()
