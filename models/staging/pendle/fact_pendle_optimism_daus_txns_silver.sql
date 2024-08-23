{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE"
    )
}}

{{ get_pendle_daus_txns_for_chain('optimism') }}