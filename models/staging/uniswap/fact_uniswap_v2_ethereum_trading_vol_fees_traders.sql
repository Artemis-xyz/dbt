{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders(
        "ethereum",
        "uniswap",
        "v2",
        "fact_uniswap_v2_ethereum_dex_swaps"
    )
}}
