{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_fork_trading_vol_fees_traders(
        "polygon", "uniswap", "v3", "fact_uniswap_v3_polygon_dex_swaps"
    )
}}