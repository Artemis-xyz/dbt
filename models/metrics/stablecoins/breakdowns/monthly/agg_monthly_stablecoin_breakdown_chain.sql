{{ config(materialized="incremental", unique_key=["date_granularity", "chain"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{ stablecoin_breakdown(["chain"], "month") }}