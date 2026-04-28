{{ config(
    materialized='external',
    location=var('data_dir') ~ '/silver/transactions',
    options={
        'partition_by': 'transaction_date',
        'overwrite_or_ignore': true
    }
) }}

{% set silver_txn_glob = var("data_dir") ~ "/silver/transactions/*/*.parquet" %}
{% set silver_txn_exists = adapter.location_exists(silver_txn_glob) %}

WITH silver_tc AS (
    SELECT transaction_code, debit_credit_indicator
    FROM read_parquet('{{ var("data_dir") }}/silver/transaction_codes/data.parquet')
),

silver_acc AS (
    SELECT account_id
    FROM read_parquet('{{ var("data_dir") }}/silver/accounts/data.parquet')
),

silver_existing_txn AS (
    {% if silver_txn_exists %}
    SELECT transaction_id
    FROM read_parquet('{{ silver_txn_glob }}')
    {% else %}
    SELECT NULL::VARCHAR AS transaction_id
    WHERE 1 = 0
    {% endif %}
),

bronze_txn AS (
    SELECT
        _source_file,
        _pipeline_run_id,
        _ingested_at,
        account_id,
        transaction_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel
    FROM read_parquet('{{ var("data_dir") }}/bronze/transactions/*/*.parquet')
),

promotable AS (
    SELECT bt.*
    FROM bronze_txn bt
    WHERE
        NOT (bt.transaction_id IS NULL OR TRIM(bt.transaction_id) = '')
        AND NOT (bt.account_id IS NULL OR TRIM(bt.account_id) = '')
        AND bt.transaction_date IS NOT NULL
        AND bt.amount IS NOT NULL
        AND NOT (bt.transaction_code IS NULL OR TRIM(bt.transaction_code) = '')
        AND NOT (bt.channel IS NULL OR TRIM(bt.channel) = '')
        AND bt.amount > 0
        AND bt.transaction_id NOT IN (SELECT transaction_id FROM silver_existing_txn)
        AND bt.transaction_code IN (SELECT transaction_code FROM silver_tc)
        AND bt.channel IN ('ONLINE', 'IN_STORE')
),

signed_txn AS (
    SELECT
        p._source_file,
        p._pipeline_run_id,
        p._ingested_at          AS _bronze_ingested_at,
        CURRENT_TIMESTAMP       AS _promoted_at,
        p.account_id,
        p.transaction_id,
        p.transaction_date,
        p.amount,
        p.transaction_code,
        p.merchant_name,
        p.channel,
        p.amount * CASE
            WHEN tc.debit_credit_indicator = 'DR' THEN 1
            WHEN tc.debit_credit_indicator = 'CR' THEN -1
        END                     AS _signed_amount,
        CASE
            WHEN sa.account_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                     AS _is_resolvable
    FROM promotable p
    JOIN silver_tc tc ON p.transaction_code = tc.transaction_code
    LEFT JOIN silver_acc sa ON p.account_id = sa.account_id
)

SELECT * FROM signed_txn
