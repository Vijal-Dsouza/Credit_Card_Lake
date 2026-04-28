select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select debit_credit_indicator
from "dbt_catalog"."main"."silver_transaction_codes"
where debit_credit_indicator is null



      
    ) dbt_internal_test