{{ config(materialized="incremental", unique_key=["date_granularity", "symbol"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{ stablecoin_breakdown(["symbol"], "week") }}
