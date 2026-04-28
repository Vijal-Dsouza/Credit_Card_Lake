create or replace view "dbt_catalog"."main"."silver_transaction_codes__dbt_int" as (
        select * from 'D:/Credit_Card_Lake/data/silver/transaction_codes/data.parquet'
    );