
{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}

SELECT
    date
    , total_supply
    , txns
    , dau
    , transfer_volume
    , chain
    , symbol
    , contract_address
FROM {{ref("agg_daily_stablecoin_metrics")}}
