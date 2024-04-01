{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_fork_unique_traders(
        "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7",
        "bsc",
        "PoolCreated",
        "pool",
        "uniswap_v3",
    )
}}
