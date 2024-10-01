
{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}

SELECT
    *
FROM {{ref("agg_daily_stablecoin_breakdown_symbol_chain")}}
