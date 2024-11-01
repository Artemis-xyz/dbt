{{ config(materialized="incremental", unique_key=["date_granularity", "chain"], snowflake_warehouse="STABLECOIN_MONTHLY") }}

{{ stablecoin_breakdown(["chain"], "week") }}