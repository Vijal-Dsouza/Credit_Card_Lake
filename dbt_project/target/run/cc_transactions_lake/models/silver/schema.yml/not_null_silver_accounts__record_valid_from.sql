select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select _record_valid_from
from "dbt_catalog"."main"."silver_accounts"
where _record_valid_from is null



      
    ) dbt_internal_test