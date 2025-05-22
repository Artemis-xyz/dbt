{{
    config(
        materialized="incremental",
        snowflake_warehouse="outerlands",
        unique_key=["date", "artemis_id"]
    )
}}

{%- set sigma_assets = dbt_utils.get_query_results_as_dict(
    "SELECT 
        artemis_id,
        coingecko_id,
        DATE(CONVERT_TIMEZONE('UTC', added_date)) as added_date,
        DATE(CONVERT_TIMEZONE('UTC', deleted_date)) as deleted_date,
        sector
     FROM " ~ source('SIGMA', 'dim_outerlands_fundamental_index_assets')
) -%}

{%- set MINIMUM_MCAP = 30000000 -%}

WITH
    eligible_months_per_asset AS (
        SELECT 
            date,
            coingecko_id,
            shifted_token_market_cap,
            DATE_TRUNC('month', date) AS month_start,
        FROM {{ ref('fact_coingecko_token_date_adjusted_gold') }}
        WHERE DATE_TRUNC('month', date) = date  -- Only keep the first day of each month
            AND shifted_token_market_cap > {{ MINIMUM_MCAP }}
    )
, combined_data AS (
    {%- for i in range(sigma_assets['ARTEMIS_ID'] | length) %}

    {%- set artemis_id = sigma_assets['ARTEMIS_ID'][i] -%}
    {%- set coingecko_id = sigma_assets['COINGECKO_ID'][i] -%}
    {%- set added_date = sigma_assets['ADDED_DATE'][i] -%}
    {%- set deleted_date = sigma_assets['DELETED_DATE'][i] %}
    {%- set sector = sigma_assets['SECTOR'][i] %}

    SELECT 
        m.date, 
        '{{ artemis_id }}' AS artemis_id,
        '{{ coingecko_id }}' AS coingecko_id,
        
        -- TXNS
        {{ sector }}_txns as txns,

        -- DAU
        {{ sector }}_dau as dau,

        -- FEES
        ecosystem_revenue as fees

        FROM {{ artemis_id }}.prod_core.ez_metrics m

    WHERE m.date >= '{{ added_date }}'
    {%- if deleted_date %}
        AND m.date < DATEADD('month', -1, DATE_TRUNC('month', DATE('{{ deleted_date }}')))
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
        SUM(txns) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_txns,
        SUM(dau) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_dau,
        SUM(fees) OVER (PARTITION BY artemis_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30d_sum_fees
    FROM combined_data
),
eligible_assets AS (
    SELECT t.*
    FROM trailing_30d_sums_by_protocol t
    JOIN eligible_months_per_asset e 
        ON e.coingecko_id = t.coingecko_id 
        AND DATE_TRUNC('month', DATEADD('day', 1, t.date)) = e.month_start
    WHERE date(t.date) = DATEADD('day', -1, DATEADD('month', 1, DATE_TRUNC('month', t.date)))
    AND t.trailing_30d_sum_txns is not null
    AND t.trailing_30d_sum_txns >= 0
    AND t.trailing_30d_sum_dau is not null
    AND t.trailing_30d_sum_dau >= 0
    AND t.trailing_30d_sum_fees is not null
    AND t.trailing_30d_sum_fees >= 0
),
total_sums AS (
    SELECT 
        date,
        SUM(trailing_30d_sum_txns) AS total_trailing_30d_sum_txns,
        SUM(trailing_30d_sum_dau) AS total_trailing_30d_sum_dau,
        SUM(trailing_30d_sum_fees) AS total_trailing_30d_sum_fees
    FROM eligible_assets
    GROUP BY date
),
trailing_and_totals AS (
    SELECT 
        DATEADD('day', 1, t.date) as date,
        t.artemis_id,
        t.coingecko_id,
        t.trailing_30d_sum_txns,
        t.trailing_30d_sum_dau,
        t.trailing_30d_sum_fees,
        tot.total_trailing_30d_sum_txns,
        tot.total_trailing_30d_sum_dau,
        tot.total_trailing_30d_sum_fees
    FROM eligible_assets t
    JOIN total_sums tot ON t.date = tot.date
)
SELECT
    t.date,
    t.artemis_id,
    t.coingecko_id,
    trailing_30d_sum_txns,
    trailing_30d_sum_dau,
    trailing_30d_sum_fees,
    trailing_30d_sum_txns / NULLIF(total_trailing_30d_sum_txns, 0) AS txns_percent_of_total,
    trailing_30d_sum_dau / NULLIF(total_trailing_30d_sum_dau, 0) AS dau_percent_of_total,
    trailing_30d_sum_fees / NULLIF(total_trailing_30d_sum_fees, 0) AS fees_percent_of_total,
    (1.0/3) * (
        COALESCE(trailing_30d_sum_txns / NULLIF(total_trailing_30d_sum_txns, 0), 0) +
        COALESCE(trailing_30d_sum_dau / NULLIF(total_trailing_30d_sum_dau, 0), 0) +
        COALESCE(trailing_30d_sum_fees / NULLIF(total_trailing_30d_sum_fees, 0), 0)
    ) AS combined_score
FROM trailing_and_totals t
WHERE 1=1
    {% if is_incremental() %}
        AND t.date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
ORDER BY date DESC, artemis_id