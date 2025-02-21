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
    date
    , chain
    , token
    , yield_fees_usd
    , yield_fees_native
FROM {{ref('fact_pendle_yield_fees_by_chain_and_token_silver')}}
where date < current_date()
