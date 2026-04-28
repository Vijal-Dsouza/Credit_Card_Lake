
    
    



select _pipeline_run_id
from "dbt_catalog"."main"."silver_transaction_codes"
where _pipeline_run_id is null


