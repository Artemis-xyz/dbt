{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        unique_key = ["tx_hash", "event_index"]
    )
}}

{{ get_pendle_amm_swaps_for_chain('base') }}