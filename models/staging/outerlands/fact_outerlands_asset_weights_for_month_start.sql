{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

{%- set sigma_assets = dbt_utils.get_query_results_as_dict(
    "SELECT * 
     FROM PC_DBT_DB.PROD.fact_sigma_example_table"
) -%}

WITH sigma_example AS (
    SELECT artemis_id, coingecko_id, symbol, added_date, deleted_date
    FROM {{ ref('fact_sigma_example_table') }}
),
combined_data AS (
    {%- for i in range(sigma_assets['ARTEMIS_ID'] | length) %}
    SELECT 
        date, 
        '{{ sigma_assets['ARTEMIS_ID'][i] }}' AS artemis_id,
        '{{ sigma_assets['COINGECKO_ID'][i] }}' AS coingecko_id,
        '{{ sigma_assets['SYMBOL'][i] }}' AS symbol,
        txns, 
        dau, 
        fees
    FROM {{ sigma_assets['ARTEMIS_ID'][i] }}.prod_core.ez_metrics
    WHERE date >= '{{ sigma_assets['ADDED_DATE'][i] }}'
    {%- if sigma_assets['DELETED_DATE'][i] %}
        AND date < '{{ sigma_assets['DELETED_DATE'][i] }}'
    {%- endif %}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
),
trailing_30d_sums_by_protocol AS (
    SELECT 
        date,
        artemis_id,
        coingecko_id,
        symbol,
        SUM(txns) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_txns,
        SUM(dau) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_dau,
        SUM(fees) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_fees
    FROM combined_data
),
total_sums AS (
    SELECT 
        date,
        SUM(trailing_30d_sum_txns) AS total_trailing_30d_sum_txns,
        SUM(trailing_30d_sum_dau) AS total_trailing_30d_sum_dau,
        SUM(trailing_30d_sum_fees) AS total_trailing_30d_sum_fees
    FROM trailing_30d_sums_by_protocol
    GROUP BY date
),
trailing_and_totals AS (
    SELECT 
        t.date,
        t.artemis_id,
        t.coingecko_id,
        t.symbol,
        t.trailing_30d_sum_txns,
        t.trailing_30d_sum_dau,
        t.trailing_30d_sum_fees,
        tot.total_trailing_30d_sum_txns,
        tot.total_trailing_30d_sum_dau,
        tot.total_trailing_30d_sum_fees
    FROM trailing_30d_sums_by_protocol t
    JOIN total_sums tot ON t.date = tot.date
    WHERE DATE_TRUNC('month', t.date) = t.date  -- First day of each month
    ORDER BY t.date DESC, t.artemis_id
)
SELECT
    date,
    artemis_id,
    coingecko_id,
    symbol,
    trailing_30d_sum_txns / NULLIF(total_trailing_30d_sum_txns, 0) AS txns_percent_of_total,
    trailing_30d_sum_dau / NULLIF(total_trailing_30d_sum_dau, 0) AS dau_percent_of_total,
    trailing_30d_sum_fees / NULLIF(total_trailing_30d_sum_fees, 0) AS fees_percent_of_total,
    (1.0/3) * (
        COALESCE(trailing_30d_sum_txns / NULLIF(total_trailing_30d_sum_txns, 0), 0) +
        COALESCE(trailing_30d_sum_dau / NULLIF(total_trailing_30d_sum_dau, 0), 0) +
        COALESCE(trailing_30d_sum_fees / NULLIF(total_trailing_30d_sum_fees, 0), 0)
    ) AS combined_score
FROM trailing_and_totals
ORDER BY date DESC, artemis_id