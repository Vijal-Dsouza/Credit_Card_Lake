

WITH bronze_accounts AS (
    SELECT
        account_id,
        customer_name,
        account_status,
        credit_limit,
        current_balance,
        open_date,
        billing_cycle_start,
        billing_cycle_end,
        _source_file,
        _ingested_at,
        _pipeline_run_id,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM read_parquet('D:/Credit_Card_Lake/data/bronze/accounts/*/*.parquet')
    WHERE
        account_id IS NOT NULL AND TRIM(account_id) != ''
        AND open_date IS NOT NULL
        AND credit_limit IS NOT NULL
        AND current_balance IS NOT NULL
        AND billing_cycle_start IS NOT NULL
        AND billing_cycle_end IS NOT NULL
        AND account_status IS NOT NULL AND TRIM(account_status) != ''
        AND account_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
),

latest_accounts AS (
    SELECT
        account_id,
        customer_name,
        account_status,
        credit_limit,
        current_balance,
        open_date,
        billing_cycle_start,
        billing_cycle_end,
        _source_file,
        _ingested_at  AS _bronze_ingested_at,
        _pipeline_run_id,
        _ingested_at  AS _record_valid_from
    FROM bronze_accounts
    WHERE _rn = 1
)

SELECT * FROM latest_accounts