--depends_on {{ ref("fact_pancakeswap_v3_ethereum_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "ethereum",
        "pancakeswap",
        "v3",
        "fact_pancakeswap_v3_ethereum_dex_swaps"
    )
}}
