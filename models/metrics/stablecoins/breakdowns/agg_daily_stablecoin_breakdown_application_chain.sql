{{ config(materialized="incremental", unique_key=["date_granularity", "chain", "application"], snowflake_warehouse="STABLECOIN_V2_LG_2") }}

{{stablecoin_breakdown(["chain", "application"])}}
