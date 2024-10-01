{{ config(materialized="incremental", unique_key=["date_granularity", "chain", "category"], snowflake_warehouse="STABLECOIN_V2_LG_2") }}

{{stablecoin_breakdown(["chain", "category"])}}
