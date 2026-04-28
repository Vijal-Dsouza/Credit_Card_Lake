
    
    



select _source_file
from "dbt_catalog"."main"."silver_quarantine"
where _source_file is null


