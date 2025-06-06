{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key = ["tx_hash", "event_index"]
    )
}}

{{ get_pendle_amm_swaps_for_chain('arbitrum', blacklist='0xb7ffe52ea584d2169ae66e7f0423574a5e15056f') }}