/*
  dim_customer
  ─────────────
  Customer dimension table for the star schema.
  One row per unique customer with all descriptive attributes.
  Grafana joins fact_transactions to this for customer-level analysis.
*/

select
    customer_id,
    cc_num,
    full_name,
    gender,
    age,
    job,
    city,
    state,
    zip,
    city_pop,
    lat,
    long

from "fraudlens"."public"."stg_customers"