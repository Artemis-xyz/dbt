{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "avalanche", "uniswap", "v3", "fact_uniswap_v3_avalanche_dex_swaps"
    )
}}
