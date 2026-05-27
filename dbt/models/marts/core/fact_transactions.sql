/*
  fact_transactions
  ──────────────────
  Central fact table of the star schema.
  One row per transaction with FK references to all dimension tables.
  Grafana joins this with dim_* tables for detailed analysis.

  Materialized as TABLE in fraudlens_dw schema — stored physically
  for fast Grafana query response times.
*/

with base as (

    select * from {{ ref('int_transaction_stats') }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

final as (

    select
        b.trans_id,
        b.customer_id,
        b.merchant_id,
        d.date_id,

        b.trans_at,
        b.amount,
        b.is_fraud,
        b.source,
        b.hour_of_day,
        b.is_weekend,
        b.ingested_at

    from base b
    left join dates d
        on date_trunc('day', b.trans_at)::date = d.full_date

)

select * from final
