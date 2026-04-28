select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select transaction_code
from "dbt_catalog"."main"."silver_transaction_codes"
where transaction_code is null



      
    ) dbt_internal_test