{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "arbitrum",
        "PoolCreated",
        "pool",
        "uniswap_v3",
    )
}}
