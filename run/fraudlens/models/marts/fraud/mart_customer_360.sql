
  
    

  create  table "fraudlens"."fraudlens_dw"."mart_customer_360__dbt_tmp"
  
  
    as
  
  (
    /*
  mart_customer_360
  ──────────────────
  Per-customer risk profile aggregated across all their transactions.
  "360" means a complete view of the customer from all angles.

  Grafana uses this for:
  - Top 10 highest risk customers
  - Customer risk distribution
  - Fraud rate by age group and job
*/

with base as (

    select * from "fraudlens"."public"."int_transaction_stats"

),

customer_stats as (

    select
        customer_id,
        customer_name,
        customer_age,
        customer_gender,
        customer_job,
        customer_state,
        customer_city,
        customer_city_pop,

        -- transaction volume
        count(*)                                    as total_transactions,
        sum(amount)                                 as total_spend,
        avg(amount)                                 as avg_transaction_amount,
        max(amount)                                 as max_transaction_amount,

        -- fraud metrics
        sum(is_fraud)                               as total_fraud_count,

        round(
            sum(is_fraud)::numeric
            / nullif(count(*), 0) * 100,
            4
        )                                           as fraud_rate_pct,

        -- most used merchant category
        mode() within group (order by merchant_category)  as top_category,

        -- date range
        min(trans_at)                               as first_transaction_at,
        max(trans_at)                               as last_transaction_at,

        -- risk tier based on fraud rate
        case
            when sum(is_fraud)::numeric / nullif(count(*), 0) > 0.05
                then 'HIGH'
            when sum(is_fraud)::numeric / nullif(count(*), 0) > 0.01
                then 'MEDIUM'
            else 'LOW'
        end                                         as risk_tier

    from base
    group by
        customer_id, customer_name, customer_age,
        customer_gender, customer_job, customer_state,
        customer_city, customer_city_pop

)

select * from customer_stats
order by fraud_rate_pct desc
  );
  