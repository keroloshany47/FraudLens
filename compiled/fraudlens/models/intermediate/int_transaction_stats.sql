/*
  int_transaction_stats
  ──────────────────────
  Joins stg_transactions with stg_customers and stg_merchants.
  Adds all the columns that both mart models need so we don't
  repeat the same join logic in every mart.

  Think of this as the "enriched fact" before final aggregation.
*/

with transactions as (

    select * from "fraudlens"."public"."stg_transactions"

),

customers as (

    select * from "fraudlens"."public"."stg_customers"

),

merchants as (

    select * from "fraudlens"."public"."stg_merchants"

),

joined as (

    select
        -- transaction core
        t.trans_id,
        t.trans_at,
        t.hour_of_day,
        t.day_of_week,
        t.is_weekend,
        t.amount,
        t.is_fraud,
        t.source,
        t.ingested_at,

        -- customer context
        t.customer_id,
        c.full_name        as customer_name,
        c.age              as customer_age,
        c.gender           as customer_gender,
        c.job              as customer_job,
        c.city             as customer_city,
        c.state            as customer_state,
        c.city_pop         as customer_city_pop,
        c.lat              as customer_lat,
        c.long             as customer_long,

        -- merchant context
        t.merchant_id,
        m.merchant_name,
        m.category         as merchant_category,
        m.merch_lat,
        m.merch_long

    from transactions t
    left join customers c on t.customer_id = c.customer_id
    left join merchants m on t.merchant_id = m.merchant_id

)

select * from joined