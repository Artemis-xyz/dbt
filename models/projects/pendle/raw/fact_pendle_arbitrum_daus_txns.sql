{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_arbitrum_daus_txns",
    )
}}

SELECT
    date
    , chain
    , dau as daus
    , daily_txns
FROM
    {{ ref('fact_pendle_arbitrum_daus_txns_silver') }}