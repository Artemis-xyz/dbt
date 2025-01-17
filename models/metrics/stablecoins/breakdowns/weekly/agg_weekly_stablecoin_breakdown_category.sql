{{ config(materialized="incremental", unique_key=["date_granularity", "category"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{ stablecoin_breakdown(["category"], 'week') }}