# dbt — Transformation Layer

dbt (data build tool) transforms raw operational data from the PostgreSQL OLTP layer into a clean analytical warehouse. It does not move data — it reads what Airflow and Spark already loaded and rebuilds it into models that Grafana and analysts query directly.

## Why dbt?

Without dbt, every analyst writes their own version of the same join. One person computes fraud rate as `fraud / total`, another as `fraud / (total - nulls)`. The numbers never match. dbt solves this by making transformations version-controlled, tested, and shared — one definition of fraud rate that every dashboard uses.

dbt also generates a **data catalog** automatically. Every model, column, test, and lineage relationship is documented and browsable at a URL. This is what companies pay Atlan or Alation thousands of dollars for — dbt gives it for free.

## Architecture

```
PostgreSQL OLTP (public schema)
    transactions · customers · merchants · fraud_alerts
              |
              | dbt reads via sources.yml
              v
    ┌─────────────────────┐
    │   staging (views)   │  stg_transactions
    │                     │  stg_customers
    │                     │  stg_merchants
    └──────────┬──────────┘
               |
               v
    ┌─────────────────────┐
    │ intermediate (views)│  int_transaction_stats
    │                     │  (joins all 3 staging models)
    └──────────┬──────────┘
               |
               v
    ┌─────────────────────────────────────┐
    │        marts (tables)               │
    │  fraudlens_dw schema                │
    │                                     │
    │  core/                              │
    │    fact_transactions  (star center) │
    │    dim_customer                     │
    │    dim_merchant                     │
    │    dim_date                         │
    │                                     │
    │  fraud/                             │
    │    mart_fraud_summary               │
    │    mart_customer_360                │
    └─────────────────────────────────────┘
              |
              | Grafana queries here
              v
    Business dashboard + pipeline health dashboard
```

## Layers explained

### Staging — `models/staging/`

Reads directly from OLTP sources. Does nothing except clean, type-cast, and rename. No business logic here. Materialized as **views** — always reflects the latest OLTP data with zero storage cost.

| Model | Source table | Key transformations |
|---|---|---|
| `stg_transactions` | `transactions` | Adds `hour_of_day`, `day_of_week`, `is_weekend` from timestamp |
| `stg_customers` | `customers` | Derives `age` from `dob`, creates `full_name` |
| `stg_merchants` | `merchants` | Normalises `category` to lowercase |

### Intermediate — `models/intermediate/`

Joins staging models together and adds derived columns needed by both mart models. Materialized as **views** — no duplication, always consistent.

| Model | What it does |
|---|---|
| `int_transaction_stats` | Joins transactions + customers + merchants into one enriched row per transaction |

### Marts — `models/marts/`

Final analytical models. Materialized as **tables** in the `fraudlens_dw` schema for fast Grafana query response.

**Core star schema:**

| Model | Purpose |
|---|---|
| `fact_transactions` | Central fact table — one row per transaction with FK references to all dims |
| `dim_customer` | Customer dimension — descriptive attributes per customer |
| `dim_merchant` | Merchant dimension — category and location per merchant |
| `dim_date` | Date dimension — one row per calendar day 2019-2021, generated with `generate_series` |

**Fraud analytics:**

| Model | Purpose |
|---|---|
| `mart_fraud_summary` | Daily fraud rate, transaction volume, avg fraud amount by category and state — primary Grafana business dashboard model |
| `mart_customer_360` | Per-customer risk profile: total spend, fraud count, fraud rate, risk tier (HIGH/MEDIUM/LOW) |

## Data quality tests

54 tests cover every model. They run automatically in CI on every pull request and in Airflow daily before dbt run.

| Test type | Count | What it checks |
|---|---|---|
| `not_null` | 22 | Required columns never contain nulls |
| `unique` | 16 | Primary keys and natural keys are never duplicated |
| `accepted_values` | 8 | `is_fraud` is only 0 or 1, `source` is only 'batch' or 'stream', `risk_tier` is only HIGH/MEDIUM/LOW |
| `accepted_range` (dbt-utils) | 8 | Amounts are between 0 and 50,000, ages are between 18 and 100, fraud rate is between 0 and 100 |

**Results on empty database:** 54/54 pass
**Results after batch load:** re-run `dbt test` — any data quality issues in the CSV surface here

## Running dbt

```bash
# Start the dbt container
docker compose --profile dbt up -d

# Install packages (dbt-utils)
docker compose exec dbt dbt deps --profiles-dir . --target dev

# Parse all SQL — syntax validation, no DB needed
docker compose exec dbt dbt parse --profiles-dir . --target dev

# Run all models (staging → intermediate → marts)
docker compose exec dbt dbt run --profiles-dir . --target dev

# Run all 54 data quality tests
docker compose exec dbt dbt test --profiles-dir . --target dev

# Run a specific model and its dependencies
docker compose exec dbt dbt run --profiles-dir . --select mart_fraud_summary+

# Generate and serve the data catalog
docker compose exec dbt dbt docs generate --profiles-dir . --target dev
docker compose exec dbt dbt docs serve --profiles-dir . --port 8083
```

Or use the Makefile shortcuts:

```bash
make dbt-run      # run all models
make dbt-test     # run all tests
make dbt-docs     # generate and serve catalog at http://localhost:8083
```

## Materialisation strategy

| Layer | Materialisation | Reason |
|---|---|---|
| Staging | `view` | Always fresh, zero storage, rebuilt instantly on every query |
| Intermediate | `view` | Same — no value in storing intermediate joins |
| Marts | `table` | Stored physically — Grafana runs dashboard queries every few seconds, views would be too slow on 1.3M rows |

## Profiles

Two targets are configured in `profiles.yml`:

- `dev` — connects to the Docker PostgreSQL container (`host: postgres`, `port: 5432`)
- `ci` — connects to the GitHub Actions PostgreSQL service container (`host: localhost`)

The Airflow DAG `dag_dbt_run` uses the `dev` target. The CI workflow uses `--target ci`.

## File structure

```
dbt/
├── dbt_project.yml          project config, materialisations, schema assignments
├── profiles.yml             database connection (dev + ci targets)
├── packages.yml             dbt-utils dependency
├── Dockerfile               python:3.11-slim + dbt-postgres==1.8.0
├── models/
│   ├── staging/
│   │   ├── sources.yml      registers OLTP tables as dbt sources
│   │   ├── _staging.yml     column docs + 18 tests
│   │   ├── stg_transactions.sql
│   │   ├── stg_customers.sql
│   │   └── stg_merchants.sql
│   ├── intermediate/
│   │   ├── _intermediate.yml  column docs + 4 tests
│   │   └── int_transaction_stats.sql
│   └── marts/
│       ├── core/
│       │   ├── _core.yml      column docs + 16 tests
│       │   ├── fact_transactions.sql
│       │   ├── dim_customer.sql
│       │   ├── dim_merchant.sql
│       │   └── dim_date.sql
│       └── fraud/
│           ├── _fraud.yml     column docs + 16 tests
│           ├── mart_fraud_summary.sql
│           └── mart_customer_360.sql
├── macros/                  empty — no custom macros needed
├── seeds/                   empty — data loaded via Airflow, not seeds
└── tests/                   empty — all tests defined inline in yml files
```

## Data catalog

After running `dbt docs generate`, a full data catalog is available at **http://localhost:8083**. It shows:

- Every model with its description and SQL
- Every column with its description and test results
- A lineage graph tracing data from OLTP source to mart
- Test pass/fail status per column

The CD workflow automatically regenerates and publishes this catalog to GitHub Pages on every merge to main. The live URL is available in the repository description.
