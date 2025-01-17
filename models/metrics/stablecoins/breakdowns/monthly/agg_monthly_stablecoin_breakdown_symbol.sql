{{ config(materialized="incremental", unique_key=["date_granularity", "symbol"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{ stablecoin_breakdown(["symbol"], "month") }}
