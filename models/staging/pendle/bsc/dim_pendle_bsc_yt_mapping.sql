{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key = "yt_address"
    )
}}

{{ get_pendle_yield_contract_creation_events('bsc') }}