{{ config(
    materialized='external',
    location=var('data_dir') ~ '/gold/daily_summary/data.parquet'
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
    CURRENT_DATE::DATE    AS transaction_date,
    0::BIGINT             AS total_transactions,
    0.00::DOUBLE          AS total_signed_amount,
    STRUCT_PACK(
        PURCHASE := STRUCT_PACK(count := 0::INTEGER, signed_amount_sum := 0.00::DOUBLE),
        PAYMENT  := STRUCT_PACK(count := 0::INTEGER, signed_amount_sum := 0.00::DOUBLE),
        FEE      := STRUCT_PACK(count := 0::INTEGER, signed_amount_sum := 0.00::DOUBLE),
        INTEREST := STRUCT_PACK(count := 0::INTEGER, signed_amount_sum := 0.00::DOUBLE)
    )                     AS transactions_by_type,
    0::BIGINT             AS online_transactions,
    0::BIGINT             AS instore_transactions,
    CURRENT_TIMESTAMP     AS _computed_at,
    'no-run'::VARCHAR     AS _pipeline_run_id,
    CURRENT_DATE::DATE    AS _source_period_start,
    CURRENT_DATE::DATE    AS _source_period_end
WHERE 1=0

{% else %}

WITH silver_tc AS (
    SELECT transaction_code, transaction_type
    FROM read_parquet('{{ var("data_dir") }}/silver/transaction_codes/data.parquet')
),

silver_txn AS (
    SELECT
        t.transaction_date,
        tc.transaction_type,
        t.channel,
        t._signed_amount,
        t._pipeline_run_id
    FROM read_parquet('{{ var("data_dir") }}/silver/transactions/*/*.parquet') t  {# F-NEW-1 #}
    JOIN silver_tc tc ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true  -- INV-04
)

SELECT
    transaction_date,
    COUNT(*)                                              AS total_transactions,
    SUM(_signed_amount)                                   AS total_signed_amount,
    STRUCT_PACK(
        PURCHASE := STRUCT_PACK(
            count             := COALESCE(SUM(CASE WHEN transaction_type = 'PURCHASE' THEN 1 END), 0)::INTEGER,
            signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type = 'PURCHASE' THEN _signed_amount END), 0.00)
        ),
        PAYMENT := STRUCT_PACK(
            count             := COALESCE(SUM(CASE WHEN transaction_type = 'PAYMENT' THEN 1 END), 0)::INTEGER,
            signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type = 'PAYMENT' THEN _signed_amount END), 0.00)
        ),
        FEE := STRUCT_PACK(
            count             := COALESCE(SUM(CASE WHEN transaction_type = 'FEE' THEN 1 END), 0)::INTEGER,
            signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type = 'FEE' THEN _signed_amount END), 0.00)
        ),
        INTEREST := STRUCT_PACK(
            count             := COALESCE(SUM(CASE WHEN transaction_type = 'INTEREST' THEN 1 END), 0)::INTEGER,
            signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type = 'INTEREST' THEN _signed_amount END), 0.00)
        )
    )                                                     AS transactions_by_type,
    COUNT(*) FILTER (WHERE channel = 'ONLINE')            AS online_transactions,
    COUNT(*) FILTER (WHERE channel = 'IN_STORE')          AS instore_transactions,
    CURRENT_TIMESTAMP                                     AS _computed_at,
    MAX(_pipeline_run_id)                                 AS _pipeline_run_id,
    MIN(transaction_date)                                 AS _source_period_start,
    MAX(transaction_date)                                 AS _source_period_end
FROM silver_txn
GROUP BY transaction_date

{% endif %}
