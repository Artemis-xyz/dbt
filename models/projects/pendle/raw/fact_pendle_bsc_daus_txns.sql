{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_bsc_daus_txns",
    )
}}

SELECT
    date
    , chain
    , dau as daus
    , daily_txns
FROM
    {{ ref('fact_pendle_bsc_daus_txns_silver') }}