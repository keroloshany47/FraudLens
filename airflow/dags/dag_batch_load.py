import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger("fraudlens.dag_batch_load")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "fraudlens"),
    "user":     os.getenv("POSTGRES_USER",     "fraudlens"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraudlens_secret"),
}
DATA_PATH  = "/opt/airflow/data/raw/fraudTrain.csv"
CHUNK_SIZE = 10_000


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


@dag(
    dag_id="dag_batch_load",
    description="Load fraudTrain.csv into PostgreSQL OLTP (historical seed)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "owner": "fraudlens",
    },
    tags=["batch", "oltp", "seed"],
)
def dag_batch_load():

    @task()
    def load_customers() -> int:
        """
        Stream CSV in chunks, flush per chunk.
        Deduplication handled by ON CONFLICT DO NOTHING — no in-memory set needed.
        """
        log.info("Loading customers chunk-by-chunk...")
        total = 0

        sql = """
            INSERT INTO customers
                (cc_num,first_name,last_name,gender,dob,
                 job,street,city,state,zip,lat,long,city_pop)
            VALUES %s
            ON CONFLICT (cc_num) DO NOTHING
        """

        for chunk in pd.read_csv(
            DATA_PATH,
            usecols=["cc_num","first","last","gender","dob",
                     "job","street","city","state","zip",
                     "lat","long","city_pop"],
            chunksize=CHUNK_SIZE,
            dtype={"zip": str},
        ):
            # rename 'long' — it's a Python built-in, itertuples() silently mangles it
            chunk = chunk.rename(columns={"long": "longitude"})
            rows = [
                (
                    int(r.cc_num), r.first, r.last, r.gender, str(r.dob),
                    r.job, r.street, r.city, r.state, str(r.zip),
                    float(r.lat), float(r.longitude), int(r.city_pop),
                )
                for r in chunk.itertuples(index=False)
            ]

            if rows:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
                    conn.commit()
                total += len(rows)

        log.info(
            "Loaded customers — %s rows attempted (dupes skipped by DB)",
            f"{total:,}",
        )
        return total

    @task()
    def load_merchants() -> int:
        """
        Stream CSV in chunks, flush per chunk.
        Deduplication handled by ON CONFLICT DO NOTHING — no in-memory set needed.
        """
        log.info("Loading merchants chunk-by-chunk...")
        total = 0

        sql = """
            INSERT INTO merchants (merchant_name,category,merch_lat,merch_long)
            VALUES %s
            ON CONFLICT (merchant_name) DO NOTHING
        """

        for chunk in pd.read_csv(
            DATA_PATH,
            usecols=["merchant","category","merch_lat","merch_long"],
            chunksize=CHUNK_SIZE,
        ):
            rows = [
                (
                    r.merchant, r.category,
                    float(r.merch_lat), float(r.merch_long),
                )
                for r in chunk.itertuples(index=False)
            ]

            if rows:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
                    conn.commit()
                total += len(rows)

        log.info(
            "Loaded merchants — %s rows attempted (dupes skipped by DB)",
            f"{total:,}",
        )
        return total

    @task()
    def load_transactions() -> int:
        """
        Load transactions in chunks using pre-built FK dicts.
        FK dicts are small (unique keys only) so keeping them in memory is fine.
        """
        log.info("Building FK lookup dicts...")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT cc_num, customer_id FROM customers")
                cc_map = {row[0]: row[1] for row in cur.fetchall()}
                cur.execute("SELECT merchant_name, merchant_id FROM merchants")
                merch_map = {row[0]: row[1] for row in cur.fetchall()}

        log.info("Loaded %s customer keys, %s merchant keys",
                 f"{len(cc_map):,}", f"{len(merch_map):,}")

        sql = """
            INSERT INTO transactions
                (trans_id,customer_id,merchant_id,
                 trans_at,unix_time,amount,is_fraud,source)
            VALUES %s
            ON CONFLICT (trans_id) DO NOTHING
        """
        total = 0
        chunk_num = 0

        for chunk in pd.read_csv(
            DATA_PATH,
            usecols=["trans_num","trans_date_trans_time","unix_time",
                     "amt","is_fraud","cc_num","merchant"],
            parse_dates=["trans_date_trans_time"],
            chunksize=CHUNK_SIZE,
        ):
            chunk_num += 1
            rows = []
            for r in chunk.itertuples(index=False):
                cid = cc_map.get(int(r.cc_num))
                mid = merch_map.get(r.merchant)
                if cid is None or mid is None:
                    continue
                rows.append((
                    r.trans_num, cid, mid,
                    r.trans_date_trans_time,
                    int(r.unix_time), float(r.amt),
                    int(r.is_fraud), "batch",
                ))

            if rows:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur, sql, rows, page_size=2000)
                    conn.commit()

            total += len(rows)
            if chunk_num % 20 == 0:
                log.info("Chunk %d done — total so far: %s",
                         chunk_num, f"{total:,}")

        log.info("Batch load complete — %s transactions", f"{total:,}")
        return total

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_run",
        trigger_dag_id="dag_dbt_run",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    customers  = load_customers()
    merchants  = load_merchants()
    txns       = load_transactions()

    customers >> merchants >> txns >> trigger_dbt


dag_batch_load()