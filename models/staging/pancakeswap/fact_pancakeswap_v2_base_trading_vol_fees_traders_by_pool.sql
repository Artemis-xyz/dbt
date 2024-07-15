--depends_on {{ ref("fact_pancakeswap_v2_base_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "base",
        "pancakeswap",
        "v2",
        "fact_pancakeswap_v2_base_dex_swaps"
    )
}}
