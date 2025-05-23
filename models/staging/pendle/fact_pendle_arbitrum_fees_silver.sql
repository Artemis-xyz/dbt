{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
    )
}}

{{ get_pendle_swap_fees_for_chain_by_token('arbitrum', blacklist='0xb7ffe52ea584d2169ae66e7f0423574a5e15056f') }}