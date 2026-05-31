/*
  dim_date
  ─────────
  Date dimension — one row per day covering the full dataset range.
  Generated using generate_series so it never needs updating.
  Grafana uses this for time-series grouping.
*/

with date_spine as (

    select
        generate_series(
            '2019-01-01'::date,
            '2021-12-31'::date,
            '1 day'::interval
        )::date as full_date

),

final as (

    select
        to_char(full_date, 'YYYYMMDD')::integer  as date_id,
        full_date,
        extract(year  from full_date)::integer   as year,
        extract(month from full_date)::integer   as month,
        extract(day   from full_date)::integer   as day,
        extract(dow   from full_date)::integer   as day_of_week,
        to_char(full_date, 'Day')                as day_name,
        to_char(full_date, 'Month')              as month_name,
        extract(quarter from full_date)::integer as quarter,
        case
            when extract(dow from full_date) in (0, 6) then true
            else false
        end                                      as is_weekend

    from date_spine

)

select * from final