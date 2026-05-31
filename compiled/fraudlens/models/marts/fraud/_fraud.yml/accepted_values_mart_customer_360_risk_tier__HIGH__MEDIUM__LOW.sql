
    
    

with all_values as (

    select
        risk_tier as value_field,
        count(*) as n_records

    from "fraudlens"."fraudlens_dw"."mart_customer_360"
    group by risk_tier

)

select *
from all_values
where value_field not in (
    'HIGH','MEDIUM','LOW'
)


