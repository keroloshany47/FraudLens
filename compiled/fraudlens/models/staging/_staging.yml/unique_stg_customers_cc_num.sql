
    
    

select
    cc_num as unique_field,
    count(*) as n_records

from "fraudlens"."public"."stg_customers"
where cc_num is not null
group by cc_num
having count(*) > 1


