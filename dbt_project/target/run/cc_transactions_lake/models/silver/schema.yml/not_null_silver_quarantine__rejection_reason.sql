select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select _rejection_reason
from "dbt_catalog"."main"."silver_quarantine"
where _rejection_reason is null



      
    ) dbt_internal_test