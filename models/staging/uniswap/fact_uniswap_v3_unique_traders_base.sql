{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "base",
        "PoolCreated",
        "pool",
        "uniswap_v3",
    )
}}
