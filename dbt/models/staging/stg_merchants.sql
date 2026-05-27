/*
  stg_merchants
  ─────────────
  Source: OLTP merchants table
  Output: one clean row per unique merchant with normalised category
*/

with source as (

    select * from {{ source('fraudlens_oltp', 'merchants') }}

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
