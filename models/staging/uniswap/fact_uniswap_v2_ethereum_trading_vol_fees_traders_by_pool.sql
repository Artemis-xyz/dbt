{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "ethereum",
        "uniswap",
        "v2",
        "fact_uniswap_v2_ethereum_dex_swaps"
    )
}}
