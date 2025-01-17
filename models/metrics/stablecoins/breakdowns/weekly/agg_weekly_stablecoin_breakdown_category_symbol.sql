{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "category"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{stablecoin_breakdown(["symbol", "category"], "week")}}
