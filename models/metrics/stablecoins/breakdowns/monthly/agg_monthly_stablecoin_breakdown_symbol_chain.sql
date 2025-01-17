{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "chain"], snowflake_warehouse="STABLECOIN_WEEKLY") }}

{{ stablecoin_breakdown(["symbol", "chain"], "month") }}
