
    
    



select _pipeline_run_id
from "dbt_catalog"."main"."silver_transactions"
where _pipeline_run_id is null


