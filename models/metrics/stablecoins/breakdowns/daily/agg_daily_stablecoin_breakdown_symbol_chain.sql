{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "chain"], snowflake_warehouse="STABLECOIN_DAILY") }}

{{ stablecoin_breakdown(["symbol", "chain"]) }}
