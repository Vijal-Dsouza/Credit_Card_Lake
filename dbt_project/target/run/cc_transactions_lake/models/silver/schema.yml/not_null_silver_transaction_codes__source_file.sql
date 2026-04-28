select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select _source_file
from "dbt_catalog"."main"."silver_transaction_codes"
where _source_file is null



      
    ) dbt_internal_test