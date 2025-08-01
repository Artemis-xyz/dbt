{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

-- Simplified ez metrics table that aggregates data by chain from the fact table
WITH chain_aggregates AS (
    SELECT 
        date
        , chain
        , protocol
        , category
        , SUM(num_restaked_eth) AS num_restaked_eth
        , SUM(amount_restaked_usd) AS amount_restaked_usd
    FROM {{ref('fact_eigenlayer_restaked_assets')}}
    GROUP BY date, chain, protocol, category
)
, avs_rewards_submitted AS (
    SELECT 
        date
        , SUM(amount_usd) AS avs_rewards_submitted
    FROM {{ ref('fact_eigenlayer_avs_rewards_submitted') }}
    WHERE event_name ilike '%AVS%'
    GROUP BY date
)
, avs_rewards_claimed AS (
    SELECT 
        date
        , SUM(amount_usd) AS avs_rewards_claimed
    FROM {{ ref('fact_eigenlayer_avs_rewards_claimed') }}
    GROUP BY date
)
, avs_and_operator_counts AS (
    SELECT
        date
        , SUM(active_operators) AS active_operators
        , SUM(active_avs) AS active_avs
    FROM {{ ref('fact_eigenlayer_avs_and_operator_counts') }}
    GROUP BY date
)
, token_incentives as (
    SELECT
        date
        , SUM(amount_aduj) AS token_incentives_native
        , SUM(amount_usd) AS token_incentives
    FROM {{ ref('fact_eigenlayer_avs_rewards_submitted') }}
    WHERE event_name not ilike '%AVS%'
    GROUP BY date
)

SELECT 
    date
    , 'eigenlayer' as artemis_id
    , chain

    -- Standardized Metrics

    -- Usage Metrics
    , avs_rewards_submitted.avs_rewards_submitted as avs_rewards_submitted
    , avs_rewards_claimed.avs_rewards_claimed as avs_rewards_claimed
    , coalesce(avs_and_operator_counts.active_operators, 0) as active_operators
    , coalesce(avs_and_operator_counts.active_avs, 0) as active_avs
    , num_restaked_eth as tvl_native
    , amount_restaked_usd as tvl

    -- Financial Metrics
    , token_incentives.token_incentives as token_incentives
    , token_incentives.token_incentives_native as token_incentives_native
FROM chain_aggregates
LEFT JOIN avs_rewards_submitted using (date)
LEFT JOIN avs_rewards_claimed using (date)
LEFT JOIN avs_and_operator_counts using (date)
LEFT JOIN token_incentives using (date)
WHERE date < CURRENT_DATE()
ORDER BY date