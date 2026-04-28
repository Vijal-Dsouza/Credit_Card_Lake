select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        _rejection_reason as value_field,
        count(*) as n_records

    from "dbt_catalog"."main"."silver_quarantine"
    group by _rejection_reason

)

select *
from all_values
where value_field not in (
    'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID','INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS'
)



      
    ) dbt_internal_test