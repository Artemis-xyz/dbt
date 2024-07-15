--depends_on {{ ref("fact_sushiswap_v2_avalanche_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "avalanche",
        "sushiswap",
        "v2",
        "fact_sushiswap_v2_avalanche_dex_swaps"
    )
}}
