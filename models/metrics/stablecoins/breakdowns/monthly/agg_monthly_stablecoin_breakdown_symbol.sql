{{ config(materialized="incremental", unique_key=["date_granularity", "symbol"], snowflake_warehouse="STABLECOIN_V2_LG_2") }}

{{ stablecoin_breakdown(["symbol"], "month") }}
