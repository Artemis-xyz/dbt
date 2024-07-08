--depends_on {{ ref("fact_fraxswap_ethereum_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX_SM",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "ethereum",
        "fraxswap",
        "v2",
        "fact_fraxswap_ethereum_dex_swaps"
    )
}}
