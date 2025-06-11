{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

-- Simplified ez metrics table that presents data by token from the fact table
SELECT 
    date
    , protocol
    , category
    , chain
    , restaking_type
    , token_symbol

    -- Crypto Metrics
    , num_restaked_tokens as tvl_native
    , amount_restaked_usd as tvl
    , num_restaked_tokens_net_change as tvl_native_net_change
    , amount_restaked_usd_net_change as tvl_net_change
FROM {{ref('fact_eigenlayer_restaked_assets')}}
ORDER BY date, token_symbol
