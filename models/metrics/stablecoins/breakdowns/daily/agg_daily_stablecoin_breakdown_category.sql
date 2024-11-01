{{ config(materialized="incremental", unique_key=["date_granularity", "category"], snowflake_warehouse="STABLECOIN_DAILY") }}

{{ stablecoin_breakdown(["category"]) }}