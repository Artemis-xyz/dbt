{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="ez_pendle_trades",
    )
}}

SELECT * FROM {{ ref("fact_pendle_trades") }}