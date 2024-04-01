{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_v3_fork_trading_vol_and_fees(
        "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "base", (), "uniswap_v3"
    )
}}
