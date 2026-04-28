{{ config(
    materialized='external',
    location=var('data_dir') ~ '/gold/weekly_account_summary/data.parquet'
) }}

{% if execute %}
  {% set silver_glob = var('data_dir') ~ '/silver/transactions/*/*.parquet' %}
  {% set result = run_query("SELECT COUNT(*) FROM glob('" ~ silver_glob ~ "')") %}
  {% set silver_txn_file_count = result.columns[0].values()[0] %}
{% else %}
  {% set silver_txn_file_count = 0 %}
{% endif %}

{% if silver_txn_file_count == 0 %}

SELECT
    CURRENT_DATE::DATE    AS week_start_date,
    CURRENT_DATE::DATE    AS week_end_date,
    ''::VARCHAR           AS account_id,
    0::BIGINT             AS total_purchases,
    NULL::DOUBLE          AS avg_purchase_amount,
    0.00::DOUBLE          AS total_payments,
    0.00::DOUBLE          AS total_fees,
    0.00::DOUBLE          AS total_interest,
    NULL::DOUBLE          AS closing_balance,
    CURRENT_TIMESTAMP     AS _computed_at,
    'no-run'::VARCHAR     AS _pipeline_run_id
WHERE 1=0

{% else %}

WITH silver_tc AS (
    SELECT transaction_code, transaction_type
    FROM read_parquet('{{ var("data_dir") }}/silver/transaction_codes/data.parquet')
),

silver_txn AS (
    SELECT
        t.account_id,
        DATE_TRUNC('week', t.transaction_date)::DATE                        AS week_start_date,
        (DATE_TRUNC('week', t.transaction_date) + INTERVAL 6 DAYS)::DATE    AS week_end_date,
        tc.transaction_type,
        t._signed_amount,
        t._pipeline_run_id
    FROM read_parquet('{{ var("data_dir") }}/silver/transactions/*/*.parquet') t  {# F-NEW-1 #}
    JOIN silver_tc tc ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true  -- INV-04
),

silver_acc AS (
    SELECT account_id, current_balance
    FROM read_parquet('{{ var("data_dir") }}/silver/accounts/data.parquet')
)

SELECT
    t.week_start_date,
    t.week_end_date,
    t.account_id,
    COUNT(*) FILTER (WHERE t.transaction_type = 'PURCHASE')                         AS total_purchases,
    AVG(t._signed_amount) FILTER (WHERE t.transaction_type = 'PURCHASE')            AS avg_purchase_amount,
    COALESCE(SUM(t._signed_amount) FILTER (WHERE t.transaction_type = 'PAYMENT'), 0) AS total_payments,
    COALESCE(SUM(t._signed_amount) FILTER (WHERE t.transaction_type = 'FEE'), 0)     AS total_fees,
    COALESCE(SUM(t._signed_amount) FILTER (WHERE t.transaction_type = 'INTEREST'), 0) AS total_interest,
    a.current_balance                                                                AS closing_balance,
    CURRENT_TIMESTAMP                                                                AS _computed_at,
    MAX(t._pipeline_run_id)                                                          AS _pipeline_run_id
FROM silver_txn t
JOIN silver_acc a ON t.account_id = a.account_id
GROUP BY t.week_start_date, t.week_end_date, t.account_id, a.current_balance

{% endif %}
