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
    , 'eigenlayer' as artemis_id
    , chain
    , category
    , restaking_type
    , token_symbol

    -- Usage Metrics
    , num_restaked_tokens as tvl_native
    , amount_restaked_usd as tvl
FROM {{ref('fact_eigenlayer_restaked_assets')}}
ORDER BY date, token_symbol
