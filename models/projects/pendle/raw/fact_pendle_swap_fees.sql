{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_swap_fees",
    )
}}


SELECT
    date
    , chain
    , symbol as token
    , fees_usd
    , fees_native
    , revenue_usd
    , revenue_native
    , supply_side_fees_usd
    , supply_side_fees_native
    , volume_usd
    , volume_native
FROM {{ref('fact_pendle_swap_fees_by_chain_and_token_silver')}}
WHERE fees_usd < 1e6
AND date < current_date()