{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE"
    )
}}

{{ get_pendle_markets_for_chain('optimism') }}