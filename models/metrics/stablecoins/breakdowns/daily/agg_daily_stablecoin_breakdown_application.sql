{{ config(materialized="incremental", unique_key=["date_granularity", "application"], snowflake_warehouse="STABLECOIN_DAILY") }}

{{ stablecoin_breakdown(["application"]) }}