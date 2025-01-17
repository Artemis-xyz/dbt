{{ config(materialized="incremental", unique_key=["date_granularity", "category", "chain"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{stablecoin_breakdown(["chain", "category"], "week")}}
