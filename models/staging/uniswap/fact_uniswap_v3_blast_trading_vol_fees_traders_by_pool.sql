{{
    config(
        materialized="table",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "blast", "uniswap", "v3", "fact_uniswap_v3_blast_dex_swaps"
    )
}}