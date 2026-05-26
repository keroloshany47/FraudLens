import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger("fraudlens.dag_batch_load")

# ── connection config ──────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "fraudlens"),
    "user":     os.getenv("POSTGRES_USER",     "fraudlens"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraudlens_secret"),
}
DATA_PATH  = "/opt/airflow/data/raw/fraudTrain.csv"
CHUNK_SIZE = 50_000   # insert 50k rows at a time to manage memory


def get_conn():
    """Return a live psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


# ── DAG definition ─────────────────────────────────────────────
@dag(
    dag_id="dag_batch_load",
    description="Load fraudTrain.csv into PostgreSQL OLTP (historical seed)",
    schedule=None,                    # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                # never run two copies at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "owner": "fraudlens",
    },
    tags=["batch", "oltp", "seed"],
)
def dag_batch_load():

    # ── TASK 1 ────────────────────────────────────────────────
    @task()
    def load_customers() -> int:
        """
        Extract every unique customer from the CSV and upsert into
        the customers table.  Uses cc_num as the natural unique key.
        ON CONFLICT DO NOTHING means re-runs are always safe.
        """
        log.info("Reading CSV for customer extraction...")
        df = pd.read_csv(DATA_PATH, usecols=[
            "cc_num", "first", "last", "gender", "dob",
            "job", "street", "city", "state", "zip",
            "lat", "long", "city_pop",
        ])

        # one row per unique credit card number
        customers = df.drop_duplicates("cc_num").reset_index(drop=True)
        log.info("Found %s unique customers", f"{len(customers):,}")

        sql = """
            INSERT INTO customers
                (cc_num, first_name, last_name, gender, dob,
                 job, street, city, state, zip, lat, long, city_pop)
            VALUES %s
            ON CONFLICT (cc_num) DO NOTHING
        """
        rows = [
            (
                int(r.cc_num), r.first, r.last, r.gender, str(r.dob),
                r.job, r.street, r.city, r.state, str(r.zip),
                float(r.lat), float(r.long), int(r.city_pop),
            )
            for _, r in customers.iterrows()
        ]

        with get_conn() as conn:
            with conn.cursor() as cur:
                # execute_values inserts all rows in one round-trip
                psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
            conn.commit()

        log.info("Upserted %s customers", f"{len(rows):,}")
        return len(rows)

    # ── TASK 2 ────────────────────────────────────────────────
    @task()
    def load_merchants() -> int:
        """
        Extract every unique merchant and upsert into merchants table.
        merchant_name is the natural unique key.
        """
        log.info("Reading CSV for merchant extraction...")
        df = pd.read_csv(DATA_PATH, usecols=[
            "merchant", "category", "merch_lat", "merch_long",
        ])

        merchants = df.drop_duplicates("merchant").reset_index(drop=True)
        log.info("Found %s unique merchants", f"{len(merchants):,}")

        sql = """
            INSERT INTO merchants
                (merchant_name, category, merch_lat, merch_long)
            VALUES %s
            ON CONFLICT (merchant_name) DO NOTHING
        """
        rows = [
            (r.merchant, r.category, float(r.merch_lat), float(r.merch_long))
            for _, r in merchants.iterrows()
        ]

        with get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
            conn.commit()

        log.info("Upserted %s merchants", f"{len(rows):,}")
        return len(rows)

    # ── TASK 3 ────────────────────────────────────────────────
    @task()
    def load_transactions() -> int:
        """
        Load all 1.3M transactions in chunks of 50k rows.
        Looks up customer_id and merchant_id from the dimension
        tables we just populated.  source='batch' marks every row
        so we can distinguish it from stream events later.
        """
        log.info("Loading transactions in chunks of %s...", f"{CHUNK_SIZE:,}")

        # pre-load lookup dictionaries into memory
        # so we don't do a DB query for every single row
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT cc_num, customer_id FROM customers")
                cc_map = {row[0]: row[1] for row in cur.fetchall()}

                cur.execute("SELECT merchant_name, merchant_id FROM merchants")
                merch_map = {row[0]: row[1] for row in cur.fetchall()}

        log.info("Loaded %s customer mappings, %s merchant mappings",
                 f"{len(cc_map):,}", f"{len(merch_map):,}")

        sql = """
            INSERT INTO transactions
                (trans_id, customer_id, merchant_id,
                 trans_at, unix_time, amount, is_fraud, source)
            VALUES %s
            ON CONFLICT (trans_id) DO NOTHING
        """

        total_inserted = 0
        chunk_num = 0

        for chunk in pd.read_csv(
            DATA_PATH,
            usecols=["trans_num", "trans_date_trans_time", "unix_time",
                     "amt", "is_fraud", "cc_num", "merchant"],
            parse_dates=["trans_date_trans_time"],
            chunksize=CHUNK_SIZE,
        ):
            chunk_num += 1
            rows = []
            for _, r in chunk.iterrows():
                customer_id = cc_map.get(int(r.cc_num))
                merchant_id = merch_map.get(r.merchant)
                if customer_id is None or merchant_id is None:
                    continue       # skip orphan rows (should not happen)
                rows.append((
                    r.trans_num,
                    customer_id,
                    merchant_id,
                    r.trans_date_trans_time,
                    int(r.unix_time),
                    float(r.amt),
                    int(r.is_fraud),
                    "batch",       # source label
                ))

            with get_conn() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, rows, page_size=5000)
                conn.commit()

            total_inserted += len(rows)
            log.info("Chunk %d: inserted %s rows (total: %s)",
                     chunk_num, f"{len(rows):,}", f"{total_inserted:,}")

        log.info("Batch load complete — %s transactions inserted", f"{total_inserted:,}")
        return total_inserted

    # ── TASK 4 ────────────────────────────────────────────────
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_run",
        trigger_dag_id="dag_dbt_run",
        wait_for_completion=False,    # don't block — let dbt run independently
        reset_dag_run=True,
    )

    # ── DEPENDENCY CHAIN ──────────────────────────────────────
    # >> means "must complete before the next task starts"
    customers  = load_customers()
    merchants  = load_merchants()
    txns       = load_transactions()

    customers >> merchants >> txns >> trigger_dbt


dag_batch_load()
