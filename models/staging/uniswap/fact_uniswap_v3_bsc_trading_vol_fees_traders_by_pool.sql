{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "bsc", "uniswap", "v3", "fact_uniswap_v3_bsc_dex_swaps"
    )
}}
