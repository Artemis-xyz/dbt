{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_yield_fees",
    )
}}


SELECT
    date,
    'yield' as type
    , chain
    , token
    , yield_fees_usd as fees
    , yield_fees_native as fees_native
FROM {{ref('fact_pendle_yield_fees_by_chain_and_token_silver')}}
where date < current_date()
UNION ALL
SELECT
    date,
    'reward' as type
    , chain
    , token
    , reward_fees_usd as fees
    , reward_fees_native as fees_native
FROM {{ref('fact_pendle_reward_fees_by_chain_and_token_silver')}}
where date < current_date()
