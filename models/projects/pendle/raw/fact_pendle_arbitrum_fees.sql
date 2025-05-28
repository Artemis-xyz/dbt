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
    , fee_usd
    , fee_native
    , volume_usd
    , volume_native
    , revenue_usd
    , revenue_native
    , supply_side_fees_usd
    , supply_side_fees_native
FROM
    {{ ref('fact_pendle_arbitrum_fees_silver') }}