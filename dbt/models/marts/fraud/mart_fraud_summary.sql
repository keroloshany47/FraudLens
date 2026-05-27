/*
  mart_fraud_summary
  ───────────────────
  Daily fraud metrics grouped by merchant category and customer state.
  This is the primary model for the Grafana business dashboard.

  Grafana queries this directly for:
  - Fraud rate over time (line chart)
  - Top fraud categories (bar chart)
  - Fraud by state (map panel)
  - Transaction volume trend
*/

with base as (

    select * from {{ ref('int_transaction_stats') }}

),

daily_agg as (

    select
        date_trunc('day', trans_at)::date         as txn_date,
        merchant_category,
        customer_state,

        -- volume metrics
        count(*)                                   as total_transactions,
        sum(amount)                                as total_amount,
        avg(amount)                                as avg_amount,

        -- fraud metrics
        sum(is_fraud)                              as fraud_count,

        -- fraud rate: what % of transactions are fraud
        -- nullif prevents division by zero
        round(
            sum(is_fraud)::numeric
            / nullif(count(*), 0) * 100,
            4
        )                                          as fraud_rate_pct,

        -- avg amount for fraud transactions only
        avg(case when is_fraud = 1 then amount end) as avg_fraud_amount,

        -- stream vs batch breakdown
        sum(case when source = 'stream' then 1 else 0 end) as stream_count,
        sum(case when source = 'batch'  then 1 else 0 end) as batch_count

    from base
    group by 1, 2, 3

)

select * from daily_agg
order by txn_date desc
