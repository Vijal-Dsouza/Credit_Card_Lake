{{ config(
    materialized='external',
    location=var('data_dir') ~ '/silver/quarantine',
    options={
        'partition_by': 'date',
        'filename_pattern': 'rejected',
        'overwrite_or_ignore': true
    }
) }}

{% set silver_txn_glob = var("data_dir") ~ "/silver/transactions/*/*.parquet" %}
{% set silver_txn_exists = adapter.location_exists(silver_txn_glob) %}

WITH silver_tc AS (
    SELECT transaction_code
    FROM read_parquet('{{ var("data_dir") }}/silver/transaction_codes/data.parquet')
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
        regexp_extract(_source_file, '(\d{4}-\d{2}-\d{2})', 1) AS date,
        account_id,
        transaction_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        CASE
            WHEN transaction_id IS NULL OR TRIM(transaction_id) = ''
              OR account_id IS NULL OR TRIM(account_id) = ''
              OR transaction_date IS NULL
              OR amount IS NULL
              OR transaction_code IS NULL OR TRIM(transaction_code) = ''
              OR channel IS NULL OR TRIM(channel) = ''
            THEN 'NULL_REQUIRED_FIELD'
            WHEN amount <= 0
            THEN 'INVALID_AMOUNT'
            WHEN transaction_id IN (SELECT transaction_id FROM silver_existing_txn)
            THEN 'DUPLICATE_TRANSACTION_ID'
            WHEN transaction_code NOT IN (SELECT transaction_code FROM silver_tc)
            THEN 'INVALID_TRANSACTION_CODE'
            WHEN channel NOT IN ('ONLINE', 'IN_STORE')
            THEN 'INVALID_CHANNEL'
        END AS _rejection_reason
    FROM read_parquet('{{ var("data_dir") }}/bronze/transactions/*/*.parquet')
),

rejected_txn AS (
    SELECT
        _source_file,
        _pipeline_run_id,
        CURRENT_TIMESTAMP      AS _rejected_at,
        _rejection_reason,
        date,
        account_id,
        transaction_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        NULL::VARCHAR          AS account_status,
        NULL::DOUBLE           AS credit_limit,
        NULL::DOUBLE           AS current_balance,
        NULL::DATE             AS open_date,
        NULL::DATE             AS billing_cycle_start,
        NULL::DATE             AS billing_cycle_end,
        NULL::VARCHAR          AS customer_name
    FROM bronze_txn
    WHERE _rejection_reason IS NOT NULL
),

bronze_acc AS (
    SELECT
        _source_file,
        _pipeline_run_id,
        regexp_extract(_source_file, '(\d{4}-\d{2}-\d{2})', 1) AS date,
        account_id,
        account_status,
        credit_limit,
        current_balance,
        open_date,
        billing_cycle_start,
        billing_cycle_end,
        customer_name,
        CASE
            WHEN account_id IS NULL OR TRIM(account_id) = ''
              OR open_date IS NULL
              OR credit_limit IS NULL
              OR current_balance IS NULL
              OR billing_cycle_start IS NULL
              OR billing_cycle_end IS NULL
              OR account_status IS NULL OR TRIM(account_status) = ''
            THEN 'NULL_REQUIRED_FIELD'
            WHEN account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
            THEN 'INVALID_ACCOUNT_STATUS'
        END AS _rejection_reason
    FROM read_parquet('{{ var("data_dir") }}/bronze/accounts/*/*.parquet')
),

rejected_acc AS (
    SELECT
        _source_file,
        _pipeline_run_id,
        CURRENT_TIMESTAMP      AS _rejected_at,
        _rejection_reason,
        date,
        account_id,
        NULL::VARCHAR          AS transaction_id,
        NULL::DATE             AS transaction_date,
        NULL::DOUBLE           AS amount,
        NULL::VARCHAR          AS transaction_code,
        NULL::VARCHAR          AS merchant_name,
        NULL::VARCHAR          AS channel,
        account_status,
        credit_limit,
        current_balance,
        open_date,
        billing_cycle_start,
        billing_cycle_end,
        customer_name
    FROM bronze_acc
    WHERE _rejection_reason IS NOT NULL
)

SELECT * FROM rejected_txn
UNION ALL
SELECT * FROM rejected_acc
