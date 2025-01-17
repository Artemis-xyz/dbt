{{ config(materialized="incremental", unique_key=["date_granularity", "chain", "category", "symbol"], snowflake_warehouse="STABLECOIN_DAILY") }}
{{ stablecoin_breakdown(["chain", "category", "symbol"]) }}
