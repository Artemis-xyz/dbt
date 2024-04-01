{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}


{{
    fact_daily_uniswap_fork_unique_traders(
        "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362",
        "ethereum",
        "PairCreated",
        "pair",
        "pancakeswap_v2",
    )
}}
