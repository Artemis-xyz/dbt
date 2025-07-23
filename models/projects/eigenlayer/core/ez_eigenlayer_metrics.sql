{{
    config(
        materialized="incremental",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

-- Simplified ez metrics table that aggregates data for eigenlayer
WITH date_spine AS (
        SELECT
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date BETWEEN 
            (SELECT MIN(date) FROM {{ ref('fact_eigenlayer_restaked_assets') }}) 
            AND (SELECT MAX(date) FROM {{ ref('fact_eigenlayer_restaked_assets') }})
)
, eigenlayer_aggregated AS (
    SELECT 
        date
        , protocol as app
        , category
        , SUM(num_restaked_eth) AS num_restaked_eth
        , SUM(amount_restaked_usd) AS amount_restaked_usd
    FROM {{ref('fact_eigenlayer_restaked_assets')}}
    {{ ez_metrics_incremental('date', backfill_date) }}
    GROUP BY date, protocol, category
)
, eigenlayer_supply_data AS (
    SELECT
        date
        , emissions_native
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply
    FROM {{ ref('fact_eigenlayer_supply_data') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, avs_rewards_submitted AS (
    SELECT 
        date
        , SUM(amount_usd) AS avs_rewards_submitted
    FROM {{ ref('fact_eigenlayer_avs_rewards_submitted') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
        AND event_name ilike '%AVS%'
    GROUP BY date
)
, avs_rewards_claimed AS (
    SELECT 
        date
        , SUM(amount_usd) AS avs_rewards_claimed
    FROM {{ ref('fact_eigenlayer_avs_rewards_claimed') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    GROUP BY date
)
, avs_and_operator_counts AS (
    SELECT
        date
        , SUM(active_operators) AS active_operators
        , SUM(active_avs) AS active_avs
    FROM {{ ref('fact_eigenlayer_avs_and_operator_counts') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    GROUP BY date
)
, market_data as (
    {{ get_coingecko_metrics('eigenlayer') }}
)
, token_incentives as (
    SELECT
        date
        , SUM(amount_aduj) AS token_incentives_native
        , SUM(amount_usd) AS token_incentives
    FROM {{ ref('fact_eigenlayer_avs_rewards_submitted') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
        AND event_name not ilike '%AVS%'
    GROUP BY date
)

SELECT 
    d.date
    , app
    , category
    -- Standarized Metrics
    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    -- Crypto Metrics
    , coalesce(avs_rewards_submitted.avs_rewards_submitted, 0) as avs_rewards_submitted
    , coalesce(avs_rewards_claimed.avs_rewards_claimed, 0) as avs_rewards_claimed
    , coalesce(avs_and_operator_counts.active_operators, 0) as active_operators
    , coalesce(avs_and_operator_counts.active_avs, 0) as active_avs
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(token_incentives.token_incentives_native, 0) as token_incentives_native
    , amount_restaked_usd as tvl
    , amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (ORDER BY date) AS tvl_net_change
    , num_restaked_eth as tvl_native
    , num_restaked_eth - LAG(num_restaked_eth) 
        OVER (ORDER BY date) AS tvl_native_net_change
    -- Supply Metrics
    , circulating_supply as circulating_supply_native
    , emissions_native as emissions_native
    , net_supply_change_native
    , premine_unlocks_native as premine_unlocks_native_native
    -- Turnover Metrics
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(market_data.token_volume, 0) as token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine d
LEFT JOIN eigenlayer_aggregated using (date)
LEFT JOIN eigenlayer_supply_data using (date)
LEFT JOIN token_incentives using (date)
LEFT JOIN avs_rewards_claimed using (date)
LEFT JOIN avs_and_operator_counts using (date)
LEFT JOIN avs_rewards_submitted using (date)
LEFT JOIN market_data using (date)
{{ ez_metrics_incremental('d.date', backfill_date) }}
    AND d.date < CURRENT_DATE()
ORDER BY d.date