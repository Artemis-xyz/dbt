--depends_on {{ ref("fact_pancakeswap_v3_arbitrum_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "arbitrum",
        "pancakeswap",
        "v3",
        "fact_pancakeswap_v3_arbitrum_dex_swaps"
    )
}}
