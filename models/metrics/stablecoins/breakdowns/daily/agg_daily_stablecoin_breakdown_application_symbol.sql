{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "application"], snowflake_warehouse="STABLECOIN_DAILY") }}

{{stablecoin_breakdown(["symbol", "application"])}}
