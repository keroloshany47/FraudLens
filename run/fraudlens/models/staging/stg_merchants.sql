
  create view "fraudlens"."public"."stg_merchants__dbt_tmp"
    
    
  as (
    /*
  stg_merchants
  ─────────────
  Source: OLTP merchants table
  Output: one clean row per unique merchant with normalised category
*/

with source as (

    select * from "fraudlens"."public"."merchants"

),

cleaned as (

    select
        merchant_id,
        merchant_name,

        -- normalise category to lowercase with underscores
        lower(trim(category))   as category,

        merch_lat,
        merch_long

    from source
    where merchant_name is not null

)

select * from cleaned
  );