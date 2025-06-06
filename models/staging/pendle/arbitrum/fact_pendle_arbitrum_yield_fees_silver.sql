{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
    )
}}

{{ get_pendle_yield_fees_for_chain_by_token('arbitrum') }}