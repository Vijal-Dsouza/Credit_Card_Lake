
    
    



select _rejection_reason
from "dbt_catalog"."main"."silver_quarantine"
where _rejection_reason is null


