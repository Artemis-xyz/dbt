
{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}


SELECT *
FROM {{ref("fact_coingecko_token_realtime_data")}}
