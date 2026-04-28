
    
    



select _pipeline_run_id
from "dbt_catalog"."main"."silver_quarantine"
where _pipeline_run_id is null


