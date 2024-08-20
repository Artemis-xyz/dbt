{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG_2") }}

{{stablecoin_breakdown(["symbol", "category"])}}
