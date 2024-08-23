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
    , fees
    , revenue
    , supply_side_fees
FROM {{ref('fact_pendle_swap_fees_by_chain_and_token_silver')}}
where date < current_date()