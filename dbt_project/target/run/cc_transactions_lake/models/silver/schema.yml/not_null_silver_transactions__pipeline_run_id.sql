select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select _pipeline_run_id
from "dbt_catalog"."main"."silver_transactions"
where _pipeline_run_id is null



      
    ) dbt_internal_test