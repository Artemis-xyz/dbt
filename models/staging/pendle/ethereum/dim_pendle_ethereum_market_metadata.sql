{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key = "market_address"
    )
}}

{{ get_pendle_markets_for_chain('ethereum') }}