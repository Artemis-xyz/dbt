{{
    config(
        materialized='incremental',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with tvl_data as (
    select
        date,
        tvl,
        tvl - LAG(tvl) 
        OVER (ORDER BY date) AS tvl_net_change
    from {{ ref('fact_babylon_tvl') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl_data) and to_date(sysdate())
)
, market_metrics AS (
    {{ get_coingecko_metrics('babylon') }}
)

SELECT
    date_spine.date
    -- Standardized Metrics
    -- Market Metrics 
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Usage Metrics
    , tvl_data.tvl
    , tvl_data.tvl_net_change
    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN market_metrics using (date)
LEFT JOIN tvl_data using (date)
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
    AND date_spine.date <= to_date(sysdate())
