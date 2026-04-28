select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select affects_balance
from "dbt_catalog"."main"."silver_transaction_codes"
where affects_balance is null



      
    ) dbt_internal_test