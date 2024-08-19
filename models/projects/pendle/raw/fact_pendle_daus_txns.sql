{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_daus_txns",
    )
}}


SELECT
    date
    , chain
    , daus
    , daily_txns
FROM {{ref('fact_pendle_daus_txns_silver')}}
where date < current_date()