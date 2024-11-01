{{ config(materialized="incremental", unique_key=["date_granularity", "symbol", "category", "chain"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{ stablecoin_breakdown(["chain", "category", "symbol"], "week") }}
