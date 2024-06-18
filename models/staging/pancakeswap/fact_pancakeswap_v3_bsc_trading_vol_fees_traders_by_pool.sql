--depends_on {{ ref("fact_pancakeswap_v3_bsc_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "bsc",
        "pancakeswap",
        "v3",
        "fact_pancakeswap_v3_bsc_dex_swaps"
    )
}}
