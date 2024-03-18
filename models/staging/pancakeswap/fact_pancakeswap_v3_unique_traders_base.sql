{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "base",
        "PoolCreated",
        "pool",
        "pancakeswap_v3",
    )
}}
