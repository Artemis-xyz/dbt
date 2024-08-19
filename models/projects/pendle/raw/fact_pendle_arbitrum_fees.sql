{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_arbitrum_fees",
    )
}}

SELECT
    date
    , chain
    , fee_usd as fees
FROM
    {{ ref('fact_pendle_arbitrum_fees_silver') }}