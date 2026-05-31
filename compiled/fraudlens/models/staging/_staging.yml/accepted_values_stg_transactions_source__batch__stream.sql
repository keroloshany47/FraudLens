
    
    

with all_values as (

    select
        source as value_field,
        count(*) as n_records

    from "fraudlens"."public"."stg_transactions"
    group by source

)

select *
from all_values
where value_field not in (
    'batch','stream'
)


