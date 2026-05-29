# Monitoring — Grafana, Prometheus & Custom Exporter

The monitoring layer gives FraudLens two complementary views into the same pipeline. Grafana renders
the business story — is fraud increasing? which customers are highest-risk? — and the engineering
story — is the pipeline healthy? is Spark dropping events? are PostgreSQL connections under
pressure? Both dashboards refresh automatically, query live data, and require no manual
configuration to start.

---

## Table of Contents

- [Why Grafana + Prometheus](#why-grafana--prometheus)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Business Dashboard](#business-dashboard)
- [Pipeline Health Dashboard](#pipeline-health-dashboard)
- [Custom Prometheus Exporter](#custom-prometheus-exporter)
- [Prometheus Scrape Configuration](#prometheus-scrape-configuration)
- [Provisioning — Zero Manual Setup](#provisioning--zero-manual-setup)
- [Access](#access)
- [Adding a Panel](#adding-a-panel)
- [Useful Commands](#useful-commands)
- [Folder Structure](#folder-structure)

---

## Why Grafana + Prometheus

Grafana connects directly to PostgreSQL via raw SQL and to Prometheus via PromQL. There is no
middleware, no ETL layer, no API between the dashboard and the data. Every panel reflects the exact
state of the database or the metric store at query time.

This has a practical consequence: the Pipeline Health dashboard can show a fraud alert count and a
DLQ depth side by side in a single view — one number from PostgreSQL, one from Prometheus — with
the same 10-second refresh. No separate tool, no context switching, no lag between sources.

Prometheus is the right choice for pipeline metrics (DLQ depth, events/sec) because these are
numeric time series that change faster than a SQL table can efficiently capture. Writing DLQ depth
to PostgreSQL every 15 seconds would pollute the operational schema with monitoring noise. Exposing
it as a Prometheus gauge keeps the concerns separated cleanly.

---

## Architecture

```
PostgreSQL OLTP (public schema)
    └── Pipeline Health panels (fraud_alerts, transactions, pg_stat_activity)

PostgreSQL OLAP (fraudlens_dw schema)
    └── Business Dashboard panels (mart_fraud_summary, mart_customer_360)

Prometheus
    ├── fraudlens-exporter :8000  →  dlq_depth, events_per_second
    ├── spark-master :8080        →  Spark executor + job metrics
    └── localhost :9090           →  Prometheus self-monitoring
        │
        └── Grafana PromQL panels (DLQ depth stat, events/sec)
```

---

## Data Sources

Three datasources are auto-configured via `provisioning/datasources/datasources.yaml` on container
startup. No manual configuration in the Grafana UI is needed.

| Name | Type | Schema | Used by |
|---|---|---|---|
| `PostgreSQL-OLTP` | PostgreSQL | `public` | Pipeline Health — live OLTP counts, fraud alerts, pg_stat_activity |
| `PostgreSQL-DW` | PostgreSQL | `fraudlens_dw` | Business Dashboard — dbt-built OLAP models |
| `Prometheus` | Prometheus | — | Pipeline Health — DLQ depth, events/sec |

Both PostgreSQL datasources point to the same `fraudlens` database on `postgres:5432`. The schema
differentiation happens at the query level (`fraudlens_dw.mart_fraud_summary` vs
`public.transactions`), not at the datasource level.

---

## Business Dashboard

**URL:** http://localhost:3000/d/fraudlens-business  
**Refresh:** every 30 seconds  
**Default time range:** 2019-01-01 → 2020-12-31 (full Sparkov dataset)  
**Data source:** PostgreSQL-DW (`fraudlens_dw` schema)

Designed for fraud analysts and business stakeholders. Every panel queries the dbt-built OLAP
models — the same numbers, consistently computed by the same SQL, that every analyst uses.

![Business Dashboard — KPIs & Trend](../../Img/Business_Dashboard1.png)

![Business Dashboard — Customer Risk & Live Alerts](../Img/Business_Dashboard2.png)

### Panels

| # | Panel | Type | Query | What it shows |
|---|---|---|---|---|
| 1 | Total Transactions | Stat (blue) | `SUM(total_transactions) FROM mart_fraud_summary` | All-time transaction count across both ingestion paths |
| 2 | Total Fraud Cases | Stat (red) | `SUM(fraud_count) FROM mart_fraud_summary` | All-time confirmed fraud count |
| 3 | Overall Fraud Rate % | Stat (green→red) | `SUM(fraud_count) / SUM(total_transactions) * 100` | Turns yellow at 0.3%, red at 0.6% |
| 4 | Avg Fraud Amount $ | Stat (orange) | `AVG(avg_fraud_amount) FROM mart_fraud_summary` | Average dollar value of fraudulent transactions |
| 5 | Stream Events | Stat (teal) | `SUM(stream_count) FROM mart_fraud_summary` | Transactions ingested via Spark streaming path |
| 6 | Daily Fraud Rate Over Time | Timeseries | `GROUP BY txn_date` | 2-year fraud rate trend — the main portfolio screenshot |
| 7 | Transaction Volume by Day | Bar chart | `SUM(total) + SUM(fraud) GROUP BY txn_date` | Total vs fraud volume side by side |
| 8 | Fraud Rate by Category | Donut chart | `SUM(fraud_count) GROUP BY merchant_category` | Which merchant categories drive the most fraud |
| 9 | Top 10 High-Risk Customers | Table | `mart_customer_360 ORDER BY fraud_rate_pct DESC LIMIT 10` | Risk tier, fraud rate, job, top category — color-coded |
| 10 | Recent Fraud Alerts | Table (OLTP live) | `fraud_alerts ORDER BY detected_at DESC LIMIT 50` | Last 50 alerts written by Spark — updates every 30s |

Panel 10 queries `PostgreSQL-OLTP` directly (not the DW) — it shows live alerts from the
operational table before dbt has had a chance to aggregate them. This is intentional: the alert
feed is the most latency-sensitive panel on the dashboard.

---

## Pipeline Health Dashboard

**URL:** http://localhost:3000/d/fraudlens-pipeline  
**Refresh:** every 10 seconds  
**Default time range:** last 24 hours  
**Data sources:** PostgreSQL-OLTP + Prometheus

Designed for data engineers. Shows the state of the pipeline itself — ingestion volumes, DLQ
health, active database connections, and risk score distribution.

![Pipeline Health](../Img/Pipeline%20Health.png)

### Panels

| # | Panel | Type | Source | What it shows |
|---|---|---|---|---|
| 1 | DLQ Messages | Stat (green/red) | Prometheus | `fraudlens_dlq_depth` — green = 0 (OK), red = any value above 0 |
| 2 | Fraud Alerts (total) | Stat (orange) | OLTP | `COUNT(*) FROM fraud_alerts` |
| 3 | Batch Rows Loaded | Stat (blue) | OLTP | `COUNT(*) WHERE source = 'batch'` |
| 4 | Stream Rows Received | Stat (teal) | OLTP | `COUNT(*) WHERE source = 'stream'` |
| 5 | PostgreSQL Connections | Stat (green→red) | OLTP | `pg_stat_activity` active connections — red above 90 |
| 6 | Transactions Ingested Over Time | Timeseries | OLTP | Batch vs stream rows by hour — shows both ingestion paths |
| 7 | Fraud Alerts Over Time | Timeseries | OLTP | Alert volume by hour — spikes indicate fraud cluster detection |
| 8 | Risk Score Distribution | Histogram | OLTP | Distribution of Spark-computed risk scores across all alerts |
| 9 | Pipeline Status | Table | OLTP | Row counts + last updated timestamp per OLTP table |

The DLQ panel (panel 1) pulls from Prometheus rather than PostgreSQL because DLQ depth is a
real-time Kafka metric — it is measured by the custom exporter by reading Kafka offsets, not by
querying a table. PostgreSQL has no visibility into Kafka topic state.

---

## Custom Prometheus Exporter

`monitoring/exporter/fraudlens_exporter.py` is a lightweight Python service that exposes
pipeline-specific metrics Prometheus cannot scrape natively from Kafka.

It runs as a dedicated Docker container (`fraudlens-exporter`) and exposes two gauges on
`:8000/metrics`:

### fraudlens_dlq_depth

```python
DLQ_DEPTH = Gauge(
    "fraudlens_dlq_depth",
    "Number of unconsumed messages in the FraudLens Dead Letter Queue topic"
)
```

Measured by reading Kafka offsets without joining a consumer group:

```python
consumer.seek_to_end(tp)
end_offset = consumer.position(tp)

consumer.seek_to_beginning(tp)
begin_offset = consumer.position(tp)

depth = end_offset - begin_offset
DLQ_DEPTH.set(depth)
```

Using `group_id=None` means the exporter is a pure observer — it does not affect consumer group
lag tracking or committed offsets in any way.

### fraudlens_events_per_second

```python
EVENTS_PER_SECOND = Gauge(
    "fraudlens_events_per_second",
    "Rate of transaction events arriving in the main Kafka topic (msgs/sec)"
)
```

Measured by comparing the current `raw_transactions` end offset to the end offset from the
previous scrape interval:

```python
delta_msgs = txn_end - prev_txn_offset
elapsed = now - prev_time
rate = delta_msgs / elapsed
EVENTS_PER_SECOND.set(rate)
```

Both metrics are scraped every 15 seconds by Prometheus and stored as time series. Grafana queries
them via PromQL: `fraudlens_dlq_depth` and `fraudlens_events_per_second`.

---

## Prometheus Scrape Configuration

`monitoring/prometheus/prometheus.yml` configures four scrape targets:

```yaml
scrape_configs:
  - job_name: prometheus          # self-monitoring
    static_configs:
      - targets: ['localhost:9090']

  - job_name: postgres            # query latency, connection pool
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: spark               # executor count, active jobs, worker health
    metrics_path: /metrics/master/prometheus
    static_configs:
      - targets: ['spark-master:8080']

  - job_name: fraudlens           # dlq_depth, events_per_second
    scrape_interval: 15s
    static_configs:
      - targets: ['fraudlens-exporter:8000']
```

Spark exposes its metrics at `/metrics/master/prometheus` on the Spark Master UI port. No
additional exporter is needed — the Spark Master serves Prometheus-format metrics natively.

---

## Provisioning — Zero Manual Setup

Both dashboards and all three datasources load automatically from files on container startup.
No manual datasource creation or dashboard import is needed.

```
monitoring/grafana/provisioning/
├── datasources/
│   └── datasources.yaml       # auto-connects PostgreSQL-OLTP + PostgreSQL-DW + Prometheus
└── dashboards/
    ├── dashboards.yaml        # tells Grafana where to find JSON files
    ├── business.json          # fraud analyst dashboard (10 panels)
    └── pipeline_health.json   # engineering dashboard (9 panels)
```

The provisioner checks for JSON changes every 30 seconds. Any edit to a dashboard JSON file is
picked up without restarting Grafana.

---

## Access

| Field | Value |
|---|---|
| URL | http://localhost:3000 |
| Username | admin |
| Password | fraudlens123 |

Business Dashboard: http://localhost:3000/d/fraudlens-business  
Pipeline Health: http://localhost:3000/d/fraudlens-pipeline  
Prometheus: http://localhost:9090  
Custom exporter metrics: http://localhost:8000/metrics  

---

## Adding a Panel

1. Open the dashboard in the Grafana UI
2. Click **Add → Visualization**
3. Write the SQL or PromQL query in the Query tab
4. Configure the panel visualization and thresholds
5. Click **Save dashboard**
6. Export the JSON: **Dashboard settings → JSON Model → Copy to clipboard**
7. Replace the JSON file in `monitoring/grafana/provisioning/dashboards/`
8. Commit — the dashboard is now version-controlled

Never treat the Grafana UI as the source of truth. The JSON files in the repository are the single
source of truth. A container reset wipes any UI-only changes — only committed JSON survives.

---

## Useful Commands

```bash
# Restart Grafana (picks up JSON changes immediately)
docker compose restart grafana

# Check Grafana logs
docker compose logs grafana -f

# Check exporter metrics directly
curl http://localhost:8000/metrics | grep fraudlens

# Check Prometheus targets (are all scrapers healthy?)
open http://localhost:9090/targets

# List dashboards via Grafana API
curl -s http://admin:fraudlens123@localhost:3000/api/search | python3 -m json.tool
```

---

## Folder Structure

```
monitoring/
├── exporter/
│   ├── Dockerfile                  # python:3.11-slim + kafka-python + prometheus-client
│   ├── fraudlens_exporter.py       # custom exporter — dlq_depth + events_per_second
│   └── requirements.txt            # kafka-python==2.0.2, prometheus-client==0.20.0
├── grafana/
│   ├── provisioning/
│   │   ├── dashboards/
│   │   │   ├── dashboards.yaml     # provisioner config
│   │   │   ├── business.json       # Business Dashboard (10 panels)
│   │   │   └── pipeline_health.json  # Pipeline Health (9 panels)
│   │   └── datasources/
│   │       └── datasources.yaml    # PostgreSQL-OLTP + PostgreSQL-DW + Prometheus
│   └── README.md
└── prometheus/
    └── prometheus.yml              # scrape config for 4 targets
```

---

*Back to root → [README.md](../../README.md)*  
*Related → [spark/spark_README.md](../../spark/spark_README.md) · [airflow/README.md](../../airflow/README.md) · [dbt/README.md](../../dbt/README.md)*
