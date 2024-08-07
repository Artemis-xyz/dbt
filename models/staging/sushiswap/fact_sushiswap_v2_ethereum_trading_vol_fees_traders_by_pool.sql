--depends_on {{ ref("fact_sushiswap_v2_ethereum_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "ethereum",
        "sushiswap",
        "v2",
        "fact_sushiswap_v2_ethereum_dex_swaps"
    )
}}
