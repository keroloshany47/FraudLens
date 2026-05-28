# FraudLens

> Real-time financial fraud detection and analytics pipeline — detects fraudulent transactions in under 5 seconds, processing 1.8M transactions across batch and streaming paths.

Financial fraud costs the global banking industry over $40 billion annually. FraudLens is a production-grade, fully containerized data engineering platform that simulates how a real bank detects fraudulent credit card transactions in real time, while maintaining a complete 2-year historical analytical warehouse for trend analysis.

---

## Architecture

![FraudLens Architecture](docs/architecture.png)

The pipeline runs two parallel ingestion paths that converge into a single PostgreSQL OLTP layer, which dbt transforms into an analytical warehouse queried by Grafana.

**Batch path** — Airflow loads `fraudTrain.csv` (1.3M labeled transactions, 2019–2020) into PostgreSQL, then triggers dbt to build the full OLAP star schema. This seeds 2 years of fraud history before a single stream event arrives.

**Streaming path** — A Python producer replays `fraudTest.csv` row by row into Kafka. Spark Structured Streaming consumes each event, computes geographic distance and a risk score using a weighted rule engine, writes enriched rows to PostgreSQL, and routes confirmed fraud events to a `fraud_alerts` Kafka topic.

Both paths write to the same `transactions` table. A `source` column (`'batch'` or `'stream'`) distinguishes them. dbt reads everything together and builds unified OLAP models on top.

---

## Stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Apache Kafka 3.7 (KRaft) | Message broker — 4 topics including Dead Letter Queue |
| Stream processing | Apache Spark 3.5 Structured Streaming | Enrichment, fraud scoring, PostgreSQL writes |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) | Batch load, dbt scheduling, DLQ monitoring |
| Transformation | dbt Core 1.8 + dbt-utils | Staging → intermediate → mart star schema |
| OLTP | PostgreSQL 15 | Operational database (transactions, customers, merchants) |
| OLAP | PostgreSQL 15 (`fraudlens_dw` schema) | Analytical warehouse (fact + dim tables, mart aggregations) |
| Visualization | Grafana 10.4 | Business dashboard + pipeline health dashboard |
| Monitoring | Prometheus 2.51 | Infrastructure metrics scraping |
| CI | GitHub Actions | dbt tests + Python lint + Spark unit tests on every PR |
| CD | GitHub Actions | Auto-publish dbt data catalog to GitHub Pages on every merge |

---

## Results

After running the full pipeline:

| Metric | Value |
|---|---|
| Total transactions processed | 1.33 Million |
| Fraud cases detected | 7,651 |
| Overall fraud rate | 0.574% |
| Average fraud amount | $498.20 |
| Stream events processed | 35.6K+ (growing) |
| dbt models built | 10 |
| dbt data quality tests | 54 (all passing) |
| Spark micro-batch interval | 5 seconds |
| Unique customers | 983 |
| Unique merchants | 693 |

---

## Quick start

### Prerequisites

- Docker Engine 24+
- Git
- 12 GB RAM minimum
- Dataset files from Kaggle (see below)

### 1. Clone and set up

```bash
git clone https://github.com/keroloshany47/FraudLens.git
cd FraudLens
bash scripts/init_repo.sh
```

### 2. Download the dataset

Download from Kaggle: [Sparkov Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)

Place both files in `data/raw/`:
```
data/raw/fraudTrain.csv    # 1.3M rows — batch/historical
data/raw/fraudTest.csv     # 550K rows — streaming simulation
```

Or use the Kaggle CLI:
```bash
pip install kaggle
kaggle datasets download -d kartik2112/fraud-detection -p data/raw/ --unzip
```

### 3. Start the stack

```bash
make setup
```

This starts all 9 services, waits for health checks, and creates all Kafka topics. Takes 2–3 minutes on first run (Docker image pulls).

### 4. Load historical data

```bash
make seed
```

Triggers the Airflow `dag_batch_load` DAG which loads 1.3M transactions into PostgreSQL, then automatically runs dbt to build the OLAP warehouse. Watch progress at **http://localhost:8082** (admin / admin).

### 5. Start the streaming pipeline

Open two terminals:

**Terminal 1 — Spark streaming job:**
```bash
make spark-submit
```

Wait for `Streaming query started — awaiting termination`.

**Terminal 2 — Kafka producer:**
```bash
make stream
```

### 6. Watch the dashboards

Open **http://localhost:3000** (admin / fraudlens123):

- **FraudLens — Business Dashboard** — fraud KPIs, daily fraud rate trend, top risky customers, live fraud alerts
- **FraudLens — Pipeline Health** — stream row count, fraud alert count, risk score distribution, pipeline status

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / fraudlens123 |
| Airflow | http://localhost:8082 | admin / admin |
| Spark Master | http://localhost:8080 | — |
| Kafka UI | http://localhost:8090 | — |
| Prometheus | http://localhost:9090 | — |
| PostgreSQL | localhost:5433 | fraudlens / fraudlens_secret |

---

## Project structure

```
FraudLens/
├── airflow/
│   ├── dags/
│   │   ├── dag_batch_load.py      # historical CSV → OLTP (idempotent)
│   │   ├── dag_dbt_run.py         # daily dbt refresh with quality gates
│   │   └── dag_dlq_monitor.py     # DLQ depth check every 15 minutes
│   └── README.md
├── dbt/
│   ├── models/
│   │   ├── staging/               # stg_transactions, stg_customers, stg_merchants
│   │   ├── intermediate/          # int_transaction_stats (enriched join)
│   │   └── marts/
│   │       ├── core/              # fact_transactions, dim_customer, dim_merchant, dim_date
│   │       └── fraud/             # mart_fraud_summary, mart_customer_360
│   └── README.md
├── kafka/
│   ├── producer/
│   │   └── stream_producer.py     # CSV → Kafka with graceful shutdown
│   └── README.md
├── spark/
│   ├── jobs/
│   │   ├── stream_processor.py    # Spark Structured Streaming job
│   │   └── utils/
│   │       ├── fraud_scorer.py    # weighted rule engine (amount + distance + category)
│   │       ├── geo_utils.py       # Haversine distance calculation
│   │       └── dlq_handler.py     # Dead Letter Queue routing
│   ├── tests/
│   │   └── test_fraud_scorer.py   # 11 unit tests — all passing
│   └── README.md
├── monitoring/
│   ├── grafana/
│   │   ├── provisioning/          # auto-provisioned datasources + dashboards
│   │   └── README.md
│   └── prometheus/
│       └── prometheus.yml
├── infra/
│   └── docker/postgres/init/
│       └── 01_schema.sql          # OLTP + OLAP schemas, auto-runs on container start
├── docs/
│   └── decisions/                 # Architecture Decision Records
│       ├── ADR-001-kafka-kraft.md
│       ├── ADR-002-microbatch.md
│       └── ADR-003-postgres-olap.md
├── .github/
│   └── workflows/
│       ├── ci.yml                 # dbt tests + ruff lint + Spark unit tests
│       └── cd.yml                 # dbt docs → GitHub Pages
├── docker-compose.yml
└── Makefile                       # make help to see all commands
```

---

## Fraud scoring logic

Each streaming transaction receives a `risk_score` between 0.0 and 1.0:

```
risk_score = (0.4 × amount_score) + (0.4 × distance_score) + (0.2 × category_score)
```

- **Amount** — transactions above $1,000 score 1.0; above $500 score 0.7
- **Distance** — customer home to merchant location via Haversine; above 500km scores 1.0
- **Category** — `shopping_net`, `misc_net`, `grocery_pos` are high-risk (0.6)

A fraud alert is written when `is_fraud = 1` (ground truth label) **or** `risk_score >= 0.7` (rule engine prediction). This separation enables precision/recall analysis in the OLAP layer.

---

## Engineering decisions

Detailed Architecture Decision Records are in `docs/decisions/`. Key choices:

**Kafka KRaft over Zookeeper** — removes the operational overhead of a separate Zookeeper cluster. KRaft is production-stable since Kafka 3.3 and is the future default.

**Micro-batch (5s) over continuous streaming** — continuous processing is still experimental in Spark 3.5. Micro-batch gives exactly-once semantics via checkpointing and roughly 40% lower CPU usage. 5-second alert latency is acceptable for fraud detection.

**PostgreSQL for both OLTP and OLAP** — a dedicated columnar store (ClickHouse, Redshift) would be faster for analytical queries but adds infrastructure complexity. At 1.3M rows, PostgreSQL with proper indexing is fast enough and eliminates a network hop between dbt and the data source.

**Dead Letter Queue** — failed Spark batches route to `dlq_transactions` instead of being silently dropped. Airflow monitors the DLQ every 15 minutes and alerts if messages accumulate.

---

## CI/CD

**CI** runs on every pull request:
- `dbt parse` + `dbt test` against a real PostgreSQL service container
- `ruff check` on all Python source files
- `pytest` on Spark unit tests (11 tests)

**CD** runs on every merge to main:
- `dbt docs generate` builds the full data catalog
- Published automatically to GitHub Pages

Live data catalog: **https://keroloshany47.github.io/FraudLens**

---

## Makefile commands

```bash
make help           # list all available commands
make setup          # first-time full stack setup
make start          # start all core services
make stop           # stop all services
make seed           # load historical data via Airflow
make stream         # start Kafka producer
make spark-submit   # start Spark streaming job
make dbt-run        # run all dbt models
make dbt-test       # run all 54 dbt tests
make dbt-docs       # generate and serve dbt catalog
make kafka-topics   # recreate all Kafka topics
make kafka-status   # show topic details
make dlq-check      # inspect dead letter queue
make status         # show all container status + URLs
make reset          # full reset (WARNING: deletes all data)
```

---

## Data source

[Sparkov Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) — 1.85M synthetic transactions generated by a simulation algorithm, covering 1,000 customers and 800 merchants across 2019–2020. Fraud rate: 0.52%. 23 features including transaction amount, customer location, merchant location, and category.

The dataset is split by role: `fraudTrain.csv` feeds the batch historical path; `fraudTest.csv` is replayed as a live stream.

---