{{ config(materialized="incremental", unique_key=["date_granularity", "chain", "category"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{stablecoin_breakdown(["chain", "category"], "month")}}
