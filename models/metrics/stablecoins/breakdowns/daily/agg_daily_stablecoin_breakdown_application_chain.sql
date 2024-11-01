{{ config(materialized="incremental", unique_key=["date_granularity", "chain", "application"], snowflake_warehouse="STABLECOIN_DAILY") }}

{{stablecoin_breakdown(["chain", "application"])}}
