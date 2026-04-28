

WITH silver_tc AS (
    SELECT transaction_code, debit_credit_indicator
    FROM read_parquet('D:/Credit_Card_Lake/data/silver/transaction_codes/data.parquet')
),

silver_acc AS (
    SELECT account_id
    FROM read_parquet('D:/Credit_Card_Lake/data/silver/accounts/data.parquet')
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
        channel,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _ingested_at
        ) AS _bronze_rn
    FROM read_parquet('D:/Credit_Card_Lake/data/bronze/transactions/*/*.parquet')
),

promotable AS (
    SELECT bt.*
    FROM bronze_txn bt
    WHERE
        bt._bronze_rn = 1
        AND NOT (bt.transaction_id IS NULL OR TRIM(bt.transaction_id) = '')
        AND NOT (bt.account_id IS NULL OR TRIM(bt.account_id) = '')
        AND bt.transaction_date IS NOT NULL
        AND bt.amount IS NOT NULL
        AND NOT (bt.transaction_code IS NULL OR TRIM(bt.transaction_code) = '')
        AND NOT (bt.channel IS NULL OR TRIM(bt.channel) = '')
        AND bt.amount > 0
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