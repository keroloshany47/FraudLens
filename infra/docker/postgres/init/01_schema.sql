-- ─────────────────────────────────────────────────────────────────
-- FraudLens — PostgreSQL Init Script
-- Runs automatically on first container start
-- Creates: fraudlens (OLTP) + fraudlens_dw (OLAP) + airflow DB
-- ─────────────────────────────────────────────────────────────────

-- Create Airflow database (separate from app data)
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO fraudlens;

-- ─── OLTP SCHEMA ────────────────────────────────────────────────
\c fraudlens;

-- Customers: unique per credit card number
CREATE TABLE IF NOT EXISTS customers (
    customer_id  SERIAL PRIMARY KEY,
    cc_num       BIGINT       UNIQUE NOT NULL,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    gender       CHAR(1)      CHECK (gender IN ('M', 'F')),
    dob          DATE,
    job          VARCHAR(200),
    street       VARCHAR(300),
    city         VARCHAR(100),
    state        CHAR(2),
    zip          VARCHAR(10),
    lat          DOUBLE PRECISION,
    long         DOUBLE PRECISION,
    city_pop     INTEGER,
    created_at   TIMESTAMPTZ  DEFAULT NOW()
);

-- Merchants: unique per name
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   SERIAL PRIMARY KEY,
    merchant_name VARCHAR(300) UNIQUE NOT NULL,
    category      VARCHAR(100),
    merch_lat     DOUBLE PRECISION,
    merch_long    DOUBLE PRECISION,
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- Transactions: core fact table (OLTP)
CREATE TABLE IF NOT EXISTS transactions (
    trans_id      VARCHAR(50)   PRIMARY KEY,         -- trans_num from Sparkov
    customer_id   INTEGER       REFERENCES customers(customer_id),
    merchant_id   INTEGER       REFERENCES merchants(merchant_id),
    trans_at      TIMESTAMPTZ   NOT NULL,
    unix_time     BIGINT,
    amount        NUMERIC(12,2) NOT NULL,
    is_fraud      SMALLINT      NOT NULL CHECK (is_fraud IN (0, 1)),
    source        VARCHAR(10)   NOT NULL CHECK (source IN ('batch', 'stream')),
    ingested_at   TIMESTAMPTZ   DEFAULT NOW()
);

-- Fraud alerts: written by Spark for is_fraud=1 stream events
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id      SERIAL PRIMARY KEY,
    trans_id      VARCHAR(50)   REFERENCES transactions(trans_id),
    detected_at   TIMESTAMPTZ   DEFAULT NOW(),
    risk_score    NUMERIC(5,4),
    distance_km   NUMERIC(10,2),
    alert_reason  VARCHAR(500)
);

-- ─── OLTP INDEXES ────────────────────────────────────────────────
-- For Grafana time-series queries
CREATE INDEX idx_txn_trans_at    ON transactions(trans_at DESC);
-- For joins in dbt staging models
CREATE INDEX idx_txn_customer_id ON transactions(customer_id);
CREATE INDEX idx_txn_merchant_id ON transactions(merchant_id);
CREATE INDEX idx_txn_is_fraud    ON transactions(is_fraud);
CREATE INDEX idx_txn_source      ON transactions(source);
-- For fraud_alerts lookups
CREATE INDEX idx_alerts_trans_id ON fraud_alerts(trans_id);
CREATE INDEX idx_alerts_detected ON fraud_alerts(detected_at DESC);

-- ─── OLAP SCHEMA (dbt writes here) ───────────────────────────────
CREATE SCHEMA IF NOT EXISTS fraudlens_dw;

-- Grant dbt full access to the DW schema
GRANT ALL ON SCHEMA fraudlens_dw TO fraudlens;
GRANT ALL ON ALL TABLES IN SCHEMA fraudlens_dw TO fraudlens;
ALTER DEFAULT PRIVILEGES IN SCHEMA fraudlens_dw
  GRANT ALL ON TABLES TO fraudlens;
ALTER DEFAULT PRIVILEGES IN SCHEMA fraudlens_dw
  GRANT ALL ON SEQUENCES TO fraudlens;

-- ─── COMMENTS (appear in dbt docs) ────────────────────────────────
COMMENT ON TABLE customers    IS 'Customer dimension — one row per unique credit card holder';
COMMENT ON TABLE merchants    IS 'Merchant dimension — one row per unique merchant name';
COMMENT ON TABLE transactions IS 'Core fact table — all transactions from batch and stream paths';
COMMENT ON TABLE fraud_alerts IS 'Real-time fraud alerts produced by Spark Structured Streaming';

COMMENT ON COLUMN transactions.source     IS 'batch = Airflow historical load | stream = Spark real-time';
COMMENT ON COLUMN transactions.ingested_at IS 'Wall-clock time this row arrived in the database';
COMMENT ON COLUMN fraud_alerts.risk_score  IS 'Spark-computed risk score 0.0-1.0';
COMMENT ON COLUMN fraud_alerts.distance_km IS 'Haversine distance between customer home and merchant location';
