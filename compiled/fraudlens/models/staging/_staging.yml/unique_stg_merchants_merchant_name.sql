
    
    

select
    merchant_name as unique_field,
    count(*) as n_records

from "fraudlens"."public"."stg_merchants"
where merchant_name is not null
group by merchant_name
having count(*) > 1


