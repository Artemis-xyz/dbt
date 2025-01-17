{{ config(materialized="incremental", unique_key=["date_granularity", "category"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{ stablecoin_breakdown(["category"], 'month') }}