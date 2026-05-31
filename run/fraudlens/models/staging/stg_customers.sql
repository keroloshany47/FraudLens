
  create view "fraudlens"."public"."stg_customers__dbt_tmp"
    
    
  as (
    /*
  stg_customers
  ─────────────
  Source: OLTP customers table
  Output: one clean row per unique customer with derived columns

  age is derived here — not stored in OLTP — so every downstream model
  gets a consistent age calculation based on the current date.
*/

with source as (

    select * from "fraudlens"."public"."customers"

),

cleaned as (

    select
        customer_id,
        cc_num,

        -- combine first and last name into one column
        trim(first_name || ' ' || last_name)        as full_name,
        first_name,
        last_name,
        gender,
        dob,

        -- derive age from date of birth
        -- date_part returns a float, cast to int for clean output
        date_part('year', age(dob::date))::integer  as age,

        job,
        city,
        state,
        zip,
        lat,
        long,
        city_pop

    from source
    where cc_num is not null

)

select * from cleaned
  );