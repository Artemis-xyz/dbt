{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "category"], snowflake_warehouse="STABLECOIN_V2_LG") }}

{{stablecoin_breakdown(["symbol", "category"])}}
