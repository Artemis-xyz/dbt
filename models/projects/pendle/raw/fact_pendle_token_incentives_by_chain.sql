{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_token_incentives_by_chain",
    )
}}


SELECT
    date
    , chain
    , amt_pendle as token_incentives_native
    , amt_usd as token_incentives
FROM
    {{ref('fact_pendle_token_incentives_by_chain_silver')}}