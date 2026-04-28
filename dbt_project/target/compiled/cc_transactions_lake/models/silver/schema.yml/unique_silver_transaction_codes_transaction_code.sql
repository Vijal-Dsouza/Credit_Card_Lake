
    
    

select
    transaction_code as unique_field,
    count(*) as n_records

from "dbt_catalog"."main"."silver_transaction_codes"
where transaction_code is not null
group by transaction_code
having count(*) > 1


