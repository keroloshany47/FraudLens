# ADR-003 — PostgreSQL as the OLAP Analytical Store

| Field       | Value                               |
|-------------|-------------------------------------|
| **Status**  | Accepted                            |
| **Date**    | 2024-01                             |
| **Decider** | FraudLens Engineering               |
| **Layer**   | Transformation / Analytical Storage |

---

## Context

FraudLens requires an analytical store to serve two Grafana dashboards that
refresh every 10–30 seconds. The store must hold the output of the dbt
transformation layer — a star schema (`fact_transactions`, `dim_customer`,
`dim_merchant`, `dim_date`) plus two fraud analytics mart tables
(`mart_fraud_summary`, `mart_customer_360`) — and answer sub-second aggregation
queries against 1.33M historical transactions.

The platform already runs PostgreSQL 15.6 as its OLTP store (the `public`
schema). The question was whether to introduce a dedicated OLAP engine
— a columnar store such as ClickHouse, DuckDB, or Apache Druid — or to
extend the existing PostgreSQL instance with a separate schema for analytical
workloads.

---

## Decision

**Use PostgreSQL 15.6 as the OLAP analytical store, isolated in a dedicated
`fraudlens_dw` schema, with dbt mart tables materialized as physical tables
for fast Grafana query response. No additional columnar store is introduced.**

---

## Rationale

### 1. The query profile fits PostgreSQL comfortably

The Grafana dashboards query pre-aggregated mart tables, not raw fact rows.
dbt materializes `mart_fraud_summary` and `mart_customer_360` as physical
tables refreshed daily. Grafana's 10–30 s refresh cycle queries these
materialized results — not the 1.33M-row `fact_transactions` table directly.

At this data volume, a B-tree index on `transaction_date` and a covering index
on `(merchant_id, is_fraud)` reduce dashboard query times to single-digit
milliseconds. PostgreSQL's parallel query planner handles the remaining
analytical aggregations without columnar compression.

### 2. Schema isolation preserves OLTP write performance

Separating OLAP from OLTP into distinct PostgreSQL schemas (`public` vs.
`fraudlens_dw`) achieves the most important property of a dedicated analytical
store: **write contention isolation**. Spark writes to `public.transactions`
every 5 seconds. dbt rebuilds `fraudlens_dw` once per day at 06:00. Neither
operation blocks the other because they touch separate table namespaces.

A columnar store would provide stronger read-path isolation, but at the cost
of an ETL hop: data would need to be extracted from PostgreSQL and loaded into
the secondary store on every dbt run. This adds latency, a new failure mode,
and an additional ~2 GB RAM service.

### 3. Single infrastructure component reduces operational complexity

The Docker Compose stack runs 12 services consuming ~9.5 GB RAM on a single
machine. Adding ClickHouse adds ~1–2 GB RAM, a new port, a new volume, new
connection credentials, a new dbt adapter (`dbt-clickhouse`), and a new
failure domain to monitor. DuckDB in-process avoids the container overhead but
introduces an incompatible SQL dialect and no concurrent write support.

Reusing PostgreSQL eliminates all of the above with no additional
infrastructure cost.

### 4. dbt + PostgreSQL is a proven, documented pairing

The `dbt-postgres` adapter is the reference adapter against which dbt Core is
developed. Every dbt feature used in FraudLens — `{{ ref() }}`, generic tests,
`generate_surrogate_key`, `dbt-utils` macros, `dbt docs generate` — works
identically on `dbt-postgres` as on any other adapter. There is no risk of
adapter-specific behaviour diverging from expectations.

### 5. Grafana PostgreSQL datasource is first-class

Grafana's built-in PostgreSQL datasource (no plugin required) supports time
series and table panel types, variable-driven filtering, and macro-based time
range interpolation (`$__timeFilter`). Switching to a columnar store would
require either a Grafana plugin or a proxy layer.

---

## Alternatives Considered

| Option | Pros | Reason Rejected |
|---|---|---|
| **ClickHouse** | Columnar, very fast aggregations, native Grafana plugin | +1 container, +~1.5 GB RAM, new adapter, new SQL dialect, overkill for 1.33M rows |
| **DuckDB (in-process)** | Zero-overhead, fastest local OLAP queries | No concurrent write support, incompatible with dbt's row-level test framework, no Grafana native connector |
| **Apache Druid** | Sub-second OLAP at billions of rows | Heavy — requires Zookeeper, ~4 GB RAM minimum, designed for 100M+ row datasets |
| **BigQuery / Snowflake** | Scalable, managed | Requires external credentials, eliminates full local reproducibility, adds cost |
| **Single PostgreSQL schema (no separation)** | Simplest | OLTP write pressure and analytical reads share the same table namespace; `VACUUM` and checkpoint contention risk |

---

## Consequences

**Positive:**
- No new services, ports, or RAM required — OLAP comes for free from the
  existing PostgreSQL container
- Full dbt feature compatibility with the reference adapter
- Grafana dashboards use the built-in PostgreSQL datasource — no plugin needed
- Schema isolation (`fraudlens_dw`) cleanly separates OLAP from OLTP without
  infrastructure overhead
- dbt docs catalog covers the full lineage from `public.transactions` through
  to `fraudlens_dw.mart_fraud_summary` in one graph

**Negative / Risks:**
- PostgreSQL's row-oriented storage means full scans of `fact_transactions`
  (1.33M rows) are slower than a columnar engine — mitigated by materializing
  all dashboard-facing queries as dbt mart tables
- If transaction volume scaled to hundreds of millions of rows, this decision
  would need to be revisited in favour of a columnar store. The `fraudlens_dw`
  schema boundary makes that migration surgical — only the dbt target adapter
  and connection profile need to change; model SQL is portable

---

## Implementation Notes

```sql
-- fraudlens_dw schema created in infra/docker/postgres/init/01_schema.sql
CREATE SCHEMA IF NOT EXISTS fraudlens_dw;

-- dbt profiles.yml — two targets, same host
fraudlens:
  outputs:
    oltp:
      type: postgres
      schema: public
    olap:
      type: postgres
      schema: fraudlens_dw   # ← dbt default target
  target: olap
```

```yaml
# dbt/models/marts/core/fact_transactions.sql materialization
{{ config(materialized='table', schema='fraudlens_dw') }}
```

dbt rebuilds all mart tables with `--full-refresh` on the daily
`dag_dbt_run` schedule. Staging models are `view` materializations
(no storage cost); intermediate and mart models are physical `table`
materializations (fast Grafana reads).

Index strategy applied post-dbt-run via an Airflow post-hook:
```sql
CREATE INDEX IF NOT EXISTS idx_fact_tx_date   ON fraudlens_dw.fact_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_fact_tx_fraud  ON fraudlens_dw.fact_transactions (is_fraud, merchant_id);
CREATE INDEX IF NOT EXISTS idx_mart_fraud_cat ON fraudlens_dw.mart_fraud_summary (category);
```

---

*See also: [ADR-001](ADR-001-kafka-kraft.md) — Kafka KRaft mode,
[ADR-002](ADR-002-microbatch.md) — Spark micro-batch interval*
