--depends_on {{ ref("fact_pancakeswap_v2_bsc_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "bsc",
        "pancakeswap",
        "v2",
        "fact_pancakeswap_v2_bsc_dex_swaps"
    )
}}
