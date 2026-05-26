# Airflow — Orchestration Layer

Apache Airflow orchestrates everything that runs on a schedule or depends on another job completing first. In FraudLens, Airflow owns three responsibilities: loading historical data, triggering dbt transformations, and monitoring pipeline health.

## Why Airflow?

A pipeline without orchestration is just a collection of scripts you run manually. Airflow turns those scripts into a managed system with automatic retries, dependency ordering, execution history, alerting, and a web UI to monitor every run. When the batch load fails halfway through, Airflow retries it automatically and logs exactly which task failed and why — without you having to watch a terminal.

We use **LocalExecutor** instead of CeleryExecutor or KubernetesExecutor. LocalExecutor runs tasks as subprocesses on the same machine as the scheduler. It requires no extra infrastructure (no Redis, no worker nodes) and handles our workload comfortably — we have at most 3 DAGs running with 6 tasks each, not hundreds of concurrent jobs.

## DAGs

### `dag_batch_load` — historical seed (manual trigger)

Loads `fraudTrain.csv` into the PostgreSQL OLTP layer. Runs once before streaming starts to seed 2 years of fraud history. Must be triggered manually from the UI or via `make seed`.

**Task order:**

```
load_customers → load_merchants → load_transactions → trigger_dbt_run
```

This order is enforced because `transactions` has foreign keys to `customers` and `merchants`. PostgreSQL rejects a transaction insert if the referenced customer or merchant does not exist yet.

**Key design decisions:**

- `ON CONFLICT DO NOTHING` on every insert — the DAG is fully idempotent. Re-running it after a crash produces the same result as the first run with no duplicates.
- `chunksize=50_000` — reads the 1.3M row CSV in 50k-row blocks to keep RAM usage under 100MB at any moment.
- Dictionary lookups for FK resolution — loads `cc_num → customer_id` and `merchant_name → merchant_id` into Python dicts once, then does O(1) lookups per row instead of one database query per row. Reduces FK resolution from 1.3M queries to 2 queries.
- `execute_values` with `page_size=5000` — inserts 5,000 rows per SQL statement instead of one per row. Roughly 100x faster than row-by-row inserts for bulk loads.

**Trigger:**

```bash
make seed
# or from Airflow UI: DAGs → dag_batch_load → Trigger DAG
```

---

### `dag_dbt_run` — daily OLAP refresh (06:00 every day)

Runs the full dbt transformation pipeline every morning to refresh the analytical warehouse with any new data that arrived overnight via the stream path.

**Task order:**

```
test_staging → run_staging → run_intermediate → run_marts → test_all → generate_docs
```

**Why test before run?** If overnight stream inserts produced broken records (null `trans_id`, negative amounts, invalid `is_fraud` values), the staging tests catch this before any mart is built on top of bad data. A broken mart silently produces wrong numbers on the Grafana dashboard — worse than no data at all.

**Layers:**

| Layer | Models | Purpose |
|---|---|---|
| staging | `stg_transactions`, `stg_customers`, `stg_merchants` | Clean, type-cast, deduplicate raw OLTP data |
| intermediate | `int_transaction_stats` | Join staging tables, derive age from dob, add category labels |
| marts | `mart_fraud_summary`, `mart_customer_360`, `fact_transactions`, `dim_*` | Final analytical models that Grafana queries |

**Schedule:** `0 6 * * *` — 6:00am every day. Change in `.env` or directly in the DAG file if you need a different time.

---

### `dag_dlq_monitor` — DLQ health check (every 15 minutes)

Monitors the `dlq_transactions` Kafka topic. If any messages have accumulated since the last check, it logs a prominent alert. In production, swap the `log.error` call with an HTTP request to Slack or PagerDuty.

**Task order:**

```
get_dlq_depth → route_on_depth → alert_team (or no_action)
```

**How it measures depth:**

Kafka stores messages as an ordered log with numeric offsets. The monitor reads the end offset (latest position) minus the beginning offset (oldest stored position). The difference is how many messages currently exist in the topic — messages that were routed to the DLQ by Spark and never consumed.

**XCom:** The depth value is passed between tasks using Airflow's built-in XCom (cross-communication) store. `get_dlq_depth` pushes the value, `route_on_depth` pulls it. No external database or file needed for inter-task communication.

**Branch operator:** `route_on_depth` returns either `"alert_team"` or `"no_action"` — Airflow runs only the matching downstream task and skips the other.

---

## Setup

Airflow initializes automatically on first `make setup`. The admin user is created with:

- **Username:** `admin`
- **Password:** `admin`

If you need to re-initialize manually:

```bash
docker exec -u root fraudlens-airflow-scheduler airflow db migrate
docker exec fraudlens-airflow-scheduler airflow users create \
  --username admin --role Admin \
  --firstname FraudLens --lastname Admin \
  --email admin@fraudlens.io --password admin
```

## Useful commands

```bash
make seed           # trigger dag_batch_load
make airflow-status # list all DAGs and their status

# From inside the container
docker exec fraudlens-airflow-scheduler airflow dags list
docker exec fraudlens-airflow-scheduler airflow dags trigger dag_batch_load
docker exec fraudlens-airflow-scheduler airflow dags trigger dag_dbt_run
```

## Web UI

Open **http://localhost:8082** — login with `admin / admin`.

All DAGs start **paused** by default. To activate a DAG, toggle the switch on the left of its name. `dag_batch_load` should be triggered manually — do not unpause it on a schedule. `dag_dbt_run` and `dag_dlq_monitor` can be unpaused to run on their schedules after the initial seed is complete.

## Folder structure

```
airflow/
├── dags/
│   ├── dag_batch_load.py    # historical CSV → OLTP
│   ├── dag_dbt_run.py       # daily dbt refresh
│   └── dag_dlq_monitor.py   # DLQ health check
├── logs/                    # auto-generated by Airflow
└── plugins/                 # empty — no custom plugins needed
```

## Dependency note

`dag_batch_load` installs `psycopg2` and `pandas` which are included in the Airflow image. `dag_dlq_monitor` requires `kafka-python` — add it to the Airflow image if it is not already present by adding a `requirements.txt` to the `airflow/` folder and mounting it in `docker-compose.yml`.
