{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

{{ get_pendle_swap_fees_for_chain_by_token('bsc') }}