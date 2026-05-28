# ADR-003: PostgreSQL as OLAP warehouse

**Status:** Accepted | **Date:** 2024-05

## Context
The OLAP layer needs to serve dbt-built aggregation models and Grafana dashboard queries. Options considered: separate PostgreSQL schema, ClickHouse, DuckDB, Redshift.

## Decision
Use a dedicated `fraudlens_dw` schema on the same PostgreSQL 15 instance, with dbt writing mart tables there.

## Reasons
- Eliminates a separate service — reduces Docker RAM usage from ~14GB to ~9GB, staying within the 12GB target
- dbt-postgres is the most mature dbt adapter with zero known compatibility issues
- Grafana's PostgreSQL plugin is battle-tested and supports both OLTP and OLAP queries from the same datasource configuration
- Mart tables contain at most ~300K rows — columnar storage is only meaningfully faster above ~10M rows for our query patterns
- No network hop between dbt and the warehouse — all transformations happen in-process

## Trade-offs accepted
- Row-oriented storage is 3–5x slower on large aggregations compared to a columnar store like ClickHouse or Redshift
- Not representative of a production-scale OLAP setup — in production this would be Snowflake, BigQuery, or Redshift
- Schema separation (`fraudlens_dw`) provides logical isolation without the operational overhead of a second database instance
