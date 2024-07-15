{{ config(materialized="table") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362", "ethereum", "pancakeswap"
    )
}}
