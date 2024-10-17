{{ config(materialized="incremental", unique_key=["date_granularity", "application"], snowflake_warehouse="STABLECOIN_V2_LG") }}

{{ stablecoin_breakdown(["application"]) }}