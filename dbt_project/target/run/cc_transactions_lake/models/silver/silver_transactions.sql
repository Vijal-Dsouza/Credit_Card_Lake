create or replace view "dbt_catalog"."main"."silver_transactions__dbt_int" as (
        select * from 'D:/Credit_Card_Lake/data/silver/transactions/*/*.parquet'
    );