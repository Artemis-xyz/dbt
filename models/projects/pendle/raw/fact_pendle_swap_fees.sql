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
    , fees_usd * 0.8 as revenue
    , fees_native * 0.8 as revenue_native
    , fees_usd * 0.2 as supply_side_fees
    , fees_native * 0.2 as supply_side_fees_native
FROM {{ref('fact_pendle_swap_fees_by_chain_and_token_silver')}}
WHERE fees_usd < 1e6
AND date < current_date()