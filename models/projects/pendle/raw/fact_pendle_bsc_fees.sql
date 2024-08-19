{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_bsc_fees",
    )
}}

SELECT
    date
    , chain
    , fee_usd as fees
FROM
    {{ ref('fact_pendle_bsc_fees_silver') }}