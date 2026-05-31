
  
    

  create  table "fraudlens"."fraudlens_dw"."dim_merchant__dbt_tmp"
  
  
    as
  
  (
    /*
  dim_merchant
  ─────────────
  Merchant dimension table for the star schema.
  One row per unique merchant with location and category.
*/

select
    merchant_id,
    merchant_name,
    category,
    merch_lat,
    merch_long

from "fraudlens"."public"."stg_merchants"
  );
  