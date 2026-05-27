# Grafana — Visualization Layer

Grafana is the observability frontend of FraudLens. It queries both PostgreSQL and Prometheus to render two purpose-built dashboards — one for fraud analysts, one for data engineers. Both dashboards are provisioned automatically from JSON files on startup, no manual configuration needed.

## Why Grafana?

Grafana connects directly to PostgreSQL via SQL and to Prometheus via PromQL — no middleware, no API layer. This means every panel you see reflects the exact state of the database at query time. The business dashboard refreshes every 30 seconds. The pipeline health dashboard refreshes every 10 seconds. Together they give you a live window into both the business story (is fraud increasing?) and the engineering story (is the pipeline healthy?).

## Dashboards

### FraudLens — Business Dashboard

**URL:** http://localhost:3000/d/fraudlens-business

Designed for a fraud analyst or business stakeholder. Queries the `fraudlens_dw` OLAP schema built by dbt.

| Panel | Type | Query source | What it shows |
|---|---|---|---|
| Total Transactions | Stat (blue) | OLAP mart | All-time transaction count |
| Total Fraud Cases | Stat (red) | OLAP mart | All-time confirmed fraud count |
| Overall Fraud Rate % | Stat (green→red) | OLAP mart | Fraud as % of all transactions — turns yellow at 0.3%, red at 0.6% |
| Avg Fraud Amount $ | Stat (orange) | OLAP mart | Average dollar value of fraudulent transactions |
| Stream Events | Stat (teal) | OLAP mart | Transactions ingested via the real-time Spark path |
| Daily Fraud Rate Over Time | Timeseries | OLAP mart | 90-day trend — main portfolio screenshot panel |
| Transaction Volume by Day | Bar chart | OLAP mart | Total vs fraud transactions side by side |
| Fraud Rate by Category | Donut chart | OLAP mart | Which merchant categories drive the most fraud |
| Top 10 High-Risk Customers | Table | OLAP mart | Risk tier, fraud rate, top category per customer — color-coded |
| Recent Fraud Alerts | Table (live) | OLTP direct | Last 50 fraud alerts written by Spark — updates every 30s |

**Refresh rate:** 30 seconds
**Time range default:** last 90 days

---

### FraudLens — Pipeline Health

**URL:** http://localhost:3000/d/fraudlens-pipeline

Designed for a data engineer. Queries both PostgreSQL OLTP and Prometheus.

| Panel | Type | Query source | What it shows |
|---|---|---|---|
| DLQ Messages | Stat (green/red) | Prometheus | Dead letter queue depth — green = OK, red = Spark failures |
| Fraud Alerts (total) | Stat (orange) | OLTP | Count of all fraud_alerts table rows |
| Batch Rows Loaded | Stat (blue) | OLTP | Transactions where source = 'batch' |
| Stream Rows Received | Stat (teal) | OLTP | Transactions where source = 'stream' |
| PostgreSQL Connections | Stat (green→red) | OLTP | Active pg_stat_activity connections — red above 90 |
| Transactions Ingested Over Time | Timeseries | OLTP | Batch vs stream rows by hour — shows both ingestion paths |
| Fraud Alerts Over Time | Timeseries | OLTP | Alert volume by hour — spikes indicate fraud cluster detection |
| Risk Score Distribution | Histogram | OLTP | Distribution of Spark-computed risk scores across all alerts |
| Pipeline Status | Table | OLTP | Row counts + last updated timestamp per table |

**Refresh rate:** 10 seconds
**Time range default:** last 24 hours

---

## Data sources

Two PostgreSQL datasources and one Prometheus datasource are auto-configured via `provisioning/datasources/datasources.yaml`:

| Name | Type | Target |
|---|---|---|
| `PostgreSQL-OLTP` | PostgreSQL | `fraudlens` db, `public` schema — raw ingested data |
| `PostgreSQL-DW` | PostgreSQL | `fraudlens` db, `fraudlens_dw` schema — dbt-built OLAP models |
| `Prometheus` | Prometheus | `http://prometheus:9090` — infrastructure metrics |

No manual setup needed. All three connect automatically on first start.

## Provisioning

Dashboards load automatically from `monitoring/grafana/provisioning/dashboards/`. The provisioner checks for changes every 30 seconds, so any edit to a JSON file is picked up without restarting Grafana.

```
monitoring/grafana/provisioning/
├── datasources/
│   └── datasources.yaml      auto-connects PostgreSQL + Prometheus
└── dashboards/
    ├── dashboards.yaml        tells Grafana where to find JSON files
    ├── business.json          fraud analyst dashboard (10 panels)
    └── pipeline_health.json   engineering dashboard (9 panels)
```

## Access

```
URL:      http://localhost:3000
Username: admin
Password: fraudlens123
```

## What the dashboards look like with data

Once `make seed` completes and `make stream` starts:

- **Business dashboard:** Total Transactions shows 1.3M+, Fraud Cases shows ~6,800, Fraud Rate shows ~0.52% (yellow), the timeseries shows 2 years of daily data, the customer risk table shows 1,000 rows color-coded by tier
- **Pipeline health:** Batch Rows jumps to 1.3M after seed, Stream Rows climbs gradually as the producer runs, Fraud Alerts Over Time shows a live growing line, DLQ stays green at 0

## Useful commands

```bash
# Restart Grafana (picks up dashboard JSON changes)
docker compose restart grafana

# Check Grafana logs
make logs-grafana

# Access Grafana API to list dashboards
curl -s http://admin:fraudlens123@localhost:3000/api/search | python3 -m json.tool
```

## Adding panels

To add a new panel:

1. Open the dashboard in Grafana UI
2. Click Add → Visualization
3. Write the SQL query in the Query tab
4. Configure the panel
5. Click Save dashboard
6. Export the JSON: Dashboard settings → JSON Model → Copy
7. Replace the JSON in `monitoring/grafana/provisioning/dashboards/business.json`
8. Commit the updated JSON — it is now version-controlled

Never rely on the Grafana UI as the source of truth. Always export and commit the JSON.
