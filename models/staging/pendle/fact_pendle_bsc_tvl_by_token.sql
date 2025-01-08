{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

{{ get_pendle_tvl_for_chain_by_token('bsc') }}