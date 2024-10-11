{{ config(materialized="incremental", unique_key=["date_granularity", "chain"], snowflake_warehouse="STABLECOIN_V2_LG") }}

{{ stablecoin_breakdown(["chain"], "week") }}