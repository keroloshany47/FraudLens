/*
  stg_transactions
  ─────────────────
  Source: OLTP transactions table (written by Airflow batch + Spark stream)
  Output: one clean row per transaction with correct types and renamed columns

  Materialized as VIEW — always reflects the latest OLTP data without
  storing a copy. Staging views are rebuilt instantly because they have
  no storage cost.
*/

with source as (

    select * from {{ source('fraudlens_oltp', 'transactions') }}

),

cleaned as (

    select
        -- identifiers
        trans_id,
        customer_id,
        merchant_id,

        -- time
        trans_at,
        unix_time,
        extract(hour  from trans_at)::integer  as hour_of_day,
        extract(dow   from trans_at)::integer  as day_of_week,  -- 0=Sunday
        case
            when extract(dow from trans_at) in (0, 6) then true
            else false
        end                                    as is_weekend,

        -- financials
        amount,

        -- labels
        is_fraud,

        -- lineage
        source,           -- 'batch' or 'stream'
        ingested_at

    from source
    where trans_id is not null  -- drop any malformed rows

)

select * from cleaned
