--depends_on {{ ref("fact_pancakeswap_v2_ethereum_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "ethereum",
        "pancakeswap",
        "v2",
        "fact_pancakeswap_v2_ethereum_dex_swaps"
    )
}}
