{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
    )
}}

{{ get_pendle_swap_fees_for_chain_by_token('base') }}