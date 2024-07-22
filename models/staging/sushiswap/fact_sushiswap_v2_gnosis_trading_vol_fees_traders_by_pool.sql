--depends_on {{ ref("fact_sushiswap_v2_gnosis_dex_swaps") }}
{{
    config(
        materialized="table",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "gnosis",
        "sushiswap",
        "v2",
        "fact_sushiswap_v2_gnosis_dex_swaps"
    )
}}
