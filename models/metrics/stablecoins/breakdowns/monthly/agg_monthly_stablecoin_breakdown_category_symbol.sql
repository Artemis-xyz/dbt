{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "category"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{stablecoin_breakdown(["symbol", "category"], "month")}}
