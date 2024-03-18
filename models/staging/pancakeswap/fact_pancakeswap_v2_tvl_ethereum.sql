{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_TVL_SM") }}


{{
    fact_daily_uniswap_v2_fork_tvl(
        "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362",
        "ethereum",
        "pancakeswap_v2",
    )
}}
