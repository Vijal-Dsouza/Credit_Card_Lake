create or replace view "dbt_catalog"."main"."silver_quarantine__dbt_int" as (
        select * from 'D:/Credit_Card_Lake/data/silver/quarantine/*/*.parquet'
    );