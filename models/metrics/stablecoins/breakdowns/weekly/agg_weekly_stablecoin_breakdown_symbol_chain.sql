{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "chain"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{ stablecoin_breakdown(["symbol", "chain"], "week") }}
