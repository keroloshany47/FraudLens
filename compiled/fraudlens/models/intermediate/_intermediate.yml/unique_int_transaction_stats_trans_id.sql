
    
    

select
    trans_id as unique_field,
    count(*) as n_records

from "fraudlens"."public"."int_transaction_stats"
where trans_id is not null
group by trans_id
having count(*) > 1


