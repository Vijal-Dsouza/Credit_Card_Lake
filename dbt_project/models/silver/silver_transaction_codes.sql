{{ config(
    materialized='external',
    location=var('data_dir') ~ '/silver/transaction_codes/data.parquet'
) }}

SELECT
    transaction_code,
    description,
    debit_credit_indicator,
    transaction_type,
    affects_balance,
    _source_file,
    _ingested_at      AS _bronze_ingested_at,
    _pipeline_run_id
FROM read_parquet('{{ var("data_dir") }}/bronze/transaction_codes/data.parquet')
