{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "optimism", "uniswap", "v3", "fact_uniswap_v3_optimism_dex_swaps"
    )
}}
